use crate::consensus::certificate::{
    CertificateTracker, SlashingReason, SynthesisCertificate, balances_root, commutative_root,
    evm_root, stateful_root,
};
use crate::consensus::dag::{DagError, ShiftStatus, VectorClockDag};
use crate::consensus::domain::{DomainRegistry, OrderingMode};
use crate::crypto::{
    AccountId, CommutativeShift, KeyPair, PoolId, RegistrationShift, Signal, StakeShift,
    StatefulShift, VectorClock,
};
use crate::evm::EvmPool;
use crate::field::coordinates::Coordinate;
use crate::field::wave_field::WaveField;
use crate::operator::{RewardPool, StakeTable, StakingConfig};
use crate::value::metabolic::MetabolicDecayEngine;
use crate::value::SupplyTracker;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// The result of applying a batch of phase-shifts.
#[derive(Clone, Debug, Default)]
pub struct SynthesisResult {
    pub commutative_applied: usize,
    pub stateful_applied: usize,
    pub evm_applied: usize,
    pub stateful_rejected: Vec<DagError>,
    pub final_balances: HashMap<AccountId, u128>,
    pub metabolic_burned: u128,
    /// Average latency (ms) from first seen to finalized for stateful + EVM shifts.
    pub avg_latency_ms: f64,
    /// Shifts processed per second during this synthesis cycle.
    pub throughput_per_sec: f64,
    /// Wall-clock duration of this synthesis cycle (ms).
    pub elapsed_ms: f64,
}

/// An oscillator node ingests phase-shifts, validates them, and synthesizes
/// the global wave-field. Commutative shifts are aggregated with NTT; stateful
/// shifts are ordered by the vector-clock DAG.
pub struct Oscillator {
    pub id: [u8; 32],
    pub wave_field: Arc<Mutex<WaveField>>,
    pub dag: Arc<Mutex<VectorClockDag>>,
    pub keypair: KeyPair,
    pub vector_clock: Arc<Mutex<VectorClock>>,
    /// Pending commutative deltas waiting for the next NTT synthesis window.
    pub pending_commutative: Arc<Mutex<Vec<(Coordinate, i128, PoolId)>>>,
    /// Pending stateful shifts awaiting DAG insertion during synthesis.
    pub pending_stateful: Arc<Mutex<Vec<StatefulShift>>>,
    pub seen_signatures: DashMap<Vec<u8>, ()>,
    pub metabolic_engine: Arc<MetabolicDecayEngine>,
    /// Monotonically increasing synthesis tick counter.
    pub synthesis_tick: AtomicU64,
    /// Known concurrency domains and their policies.
    pub domain_registry: Arc<RwLock<DomainRegistry>>,
    /// Optional operator keypair used to sign synthesis certificates.
    pub operator_keypair: Option<KeyPair>,
    /// Signed synthesis certificates indexed by tick.
    pub certificates: Arc<RwLock<HashMap<u64, SynthesisCertificate>>>,
    /// Operator stake table controlling certificate eligibility.
    pub stake_table: Arc<StakeTable>,
    /// Tracks observed peer certificates and detects equivocation.
    pub certificate_tracker: Arc<CertificateTracker>,
    /// Accrued operator rewards from metabolic burn.
    pub reward_pool: Arc<RwLock<RewardPool>>,
    /// EVM transaction pool.
    pub evm_pool: Arc<Mutex<EvmPool>>,
    /// Tracks circulating and burned WAVE supply.
    pub supply_tracker: Arc<SupplyTracker>,
}

impl Oscillator {
    pub fn new(id: [u8; 32], ntt_size: usize) -> Self {
        Self::new_with_keypair(id, ntt_size, KeyPair::generate())
    }

    pub fn new_with_keypair(id: [u8; 32], ntt_size: usize, keypair: KeyPair) -> Self {
        Self {
            id,
            wave_field: Arc::new(Mutex::new(WaveField::new(ntt_size))),
            dag: Arc::new(Mutex::new(VectorClockDag::new())),
            keypair,
            vector_clock: Arc::new(Mutex::new(VectorClock::new())),
            pending_commutative: Arc::new(Mutex::new(Vec::new())),
            pending_stateful: Arc::new(Mutex::new(Vec::new())),
            seen_signatures: DashMap::new(),
            metabolic_engine: Arc::new(MetabolicDecayEngine::new()),
            synthesis_tick: AtomicU64::new(0),
            domain_registry: Arc::new(RwLock::new(DomainRegistry::new())),
            operator_keypair: None,
            certificates: Arc::new(RwLock::new(HashMap::new())),
            stake_table: Arc::new(StakeTable::new(StakingConfig::default())),
            certificate_tracker: Arc::new(CertificateTracker::new()),
            reward_pool: Arc::new(RwLock::new(RewardPool::new())),
            evm_pool: Arc::new(Mutex::new(EvmPool::new())),
            supply_tracker: Arc::new(SupplyTracker::new()),
        }
    }

    pub fn new_with_stake(
        id: [u8; 32],
        ntt_size: usize,
        keypair: KeyPair,
        stake_table: StakeTable,
    ) -> Self {
        let mut osc = Self::new_with_keypair(id, ntt_size, keypair.clone());
        osc.operator_keypair = Some(keypair);
        osc.stake_table = Arc::new(stake_table);
        osc
    }

    pub fn set_operator_keypair(&mut self, keypair: KeyPair) {
        self.operator_keypair = Some(keypair);
    }

    /// Ingest a peer synthesis certificate.  Conflicting certificates from the
    /// same operator and tick slash the operator.
    pub fn ingest_certificate(
        &self,
        cert: SynthesisCertificate,
        key_registry: &HashMap<AccountId, ed25519_dalek::VerifyingKey>,
    ) -> Result<(), SlashingReason> {
        let stake_table = self.stake_table.clone();
        let supply_tracker = self.supply_tracker.clone();
        let stake_checker = |op: &AccountId| stake_table.is_staked(op);
        let stake_amount = |op: &AccountId| stake_table.get_stake(op);
        let mut slash = |op: AccountId| {
            let (_, burned) = stake_table.slash(op);
            supply_tracker.burn(burned);
        };
        self.certificate_tracker
            .apply(cert, key_registry, &stake_checker, &stake_amount, &mut slash)
    }

    /// Check whether a stake-weighted quorum of certificates exists for `tick`.
    pub fn check_quorum(&self, tick: u64) -> Option<(crate::consensus::certificate::QuorumView, u128)> {
        let threshold = self.stake_table.quorum_threshold();
        self.certificate_tracker.check_quorum(tick, threshold)
    }

    pub fn seed_account(&self, account: AccountId, amount: u128) {
        // Enforce the fixed 1B WAVE supply cap at the point of minting.
        if !self.supply_tracker.mint(amount) {
            tracing::warn!(
                "seed_account for {} rejected: would exceed {} WAVE supply cap",
                account,
                crate::value::supply::TOTAL_WAVE_SUPPLY / crate::field::wave_field::WAVE_PRECISION
            );
            return;
        }
        // Always acquire dag before wave_field to keep a consistent lock order
        // with synthesis (which locks dag then wave_field).
        let mut dag = self.dag.lock().unwrap();
        dag.seed_balance(account, amount);
        drop(dag);
        let field = self.wave_field.lock().unwrap();
        field.credit_account(account, amount);
    }

    /// Mark an account as holding non-WAVE value (e.g. USDC or a bridged asset)
    /// so it is exempt from metabolic decay.  Metabolic decay is WAVE's monetary
    /// policy; foreign value must hold its worth.
    pub fn mark_non_decaying(&self, account: AccountId) {
        let field = self.wave_field.lock().unwrap();
        field.set_non_decaying(account);
    }

    /// Ingest a single phase-shift. Deduplicates and queues for the next
    /// synthesis cycle.
    pub fn ingest(&self, shift: Signal) -> Result<(), String> {
        match shift {
            Signal::Commutative(c) => self.ingest_commutative(c),
            Signal::Stateful(s) => self.ingest_stateful(s),
            Signal::Registration(_) => Ok(()), // registrations are applied immediately
            Signal::Stake(_) => Err("stake signals must be applied via apply_stake".to_string()),
            Signal::Ping { .. } | Signal::Pong { .. } => Ok(()), // network probes, not state
            Signal::Certificate(_) => Ok(()), // certificates are applied via ingest_certificate
            Signal::Auth { .. } => Ok(()),     // gossip-layer authentication, not state
        }
    }

    /// Apply a stake event.  Verifies the operator signature and updates the
    /// local stake table.  In the current testnet implementation the signed
    /// stake announcement is trusted; nodes that have not yet synced the
    /// operator's on-chain balance still learn the stake so they can verify
    /// synthesis certificates from peers that join before them.
    pub fn apply_stake(&self, stake: &StakeShift) -> bool {
        if !stake.verify() {
            tracing::warn!("stake rejected for {}: invalid signature", stake.operator);
            return false;
        }

        let previous_locked = self.stake_table.get_stake(&stake.operator);
        if stake.amount == previous_locked {
            return true;
        }

        // Lock order: dag then wave_field, consistent with synthesis.
        let mut dag = self.dag.lock().unwrap();
        let field = self.wave_field.lock().unwrap();

        if stake.amount > previous_locked {
            let additional = stake.amount - previous_locked;
            if field.account_balance(stake.operator).units < additional {
                tracing::warn!(
                    "stake rejected for {}: insufficient liquid balance (need {}, have {})",
                    stake.operator,
                    additional,
                    field.account_balance(stake.operator).units
                );
                return false;
            }
            if !field.debit_account(stake.operator, additional) {
                return false;
            }
            *dag.balances.entry(stake.operator).or_insert(0) = dag
                .balances
                .get(&stake.operator)
                .copied()
                .unwrap_or(0)
                .saturating_sub(additional);
        } else {
            let refund = previous_locked - stake.amount;
            field.credit_account(stake.operator, refund);
            *dag.balances.entry(stake.operator).or_insert(0) += refund;
        }

        drop(field);
        drop(dag);

        self.stake_table.stake(stake.operator, stake.amount);
        true
    }

    /// Compute the execution fee for a stateful signal according to the fee
    /// policy of its domain.  Returns the fee in sub-units and the post-fee
    /// transfer amount.
    pub fn compute_signal_fee(
        &self,
        shift: &StatefulShift,
    ) -> Result<(u128, u128), String> {
        use crate::consensus::domain::FeePolicy;
        let policy = self
            .domain_registry
            .read()
            .unwrap()
            .get(&shift.domain)
            .cloned()
            .ok_or_else(|| format!("unknown domain {}", hex::encode(shift.domain)))?;
        match policy.fee_policy {
            FeePolicy::Flat(fee) => {
                if shift.amount < fee {
                    return Err("transfer amount does not cover flat fee".to_string());
                }
                Ok((fee, shift.amount - fee))
            }
            FeePolicy::Percentage(bp) => {
                let fee = shift.amount.saturating_mul(bp as u128) / 10_000;
                Ok((fee, shift.amount - fee))
            }
            FeePolicy::MetabolicOnly => Ok((0, shift.amount)),
        }
    }

    /// Apply a registration event directly so every node learns the account.
    /// The caller must register the public key in the API registry separately.
    pub fn apply_registration(&self, reg: &RegistrationShift) {
        // Keep lock order consistent with synthesis: dag first, then wave_field.
        let mut dag = self.dag.lock().unwrap();
        dag.seed_balance(reg.wave_account, 10_000_000_000_000);
        dag.seed_balance(reg.usdc_account, 10_000_000_000_000);
        drop(dag);
        let field = self.wave_field.lock().unwrap();
        field.ensure_account(reg.account);
        field.ensure_account(reg.wave_account);
        field.ensure_account(reg.usdc_account);
        // USDC is foreign value and must not metabolically decay.
        field.set_non_decaying(reg.usdc_account);
        if field.account_balance(reg.wave_account).units == 0 {
            field.credit_account(reg.wave_account, 10_000_000_000_000);
        }
        if field.account_balance(reg.usdc_account).units == 0 {
            field.credit_account(reg.usdc_account, 10_000_000_000_000);
        }
    }

    fn ingest_commutative(&self, shift: CommutativeShift) -> Result<(), String> {
        let policy = self
            .domain_registry
            .read()
            .unwrap()
            .get(&shift.domain)
            .cloned()
            .ok_or_else(|| format!("unknown domain {}", hex::encode(shift.domain)))?;
        if !policy.commutative {
            return Err(format!(
                "domain {} does not allow commutative signals",
                hex::encode(shift.domain)
            ));
        }
        if self.seen_signatures.contains_key(&shift.signature) {
            return Ok(()); // already processed
        }
        self.seen_signatures.insert(shift.signature, ());
        let mut pending = self.pending_commutative.lock().unwrap();
        // Latency for commutative signals is tracked by the batch synthesis
        // interval; individual first-seen times are not recorded.
        pending.push((shift.coordinate, shift.delta, shift.pool_id));
        Ok(())
    }

    fn ingest_stateful(&self, mut shift: StatefulShift) -> Result<(), String> {
        let policy = self
            .domain_registry
            .read()
            .unwrap()
            .get(&shift.domain)
            .cloned()
            .ok_or_else(|| format!("unknown domain {}", hex::encode(shift.domain)))?;
        if !policy.stateful {
            return Err(format!(
                "domain {} does not allow stateful signals",
                hex::encode(shift.domain)
            ));
        }
        if policy.ordering == OrderingMode::Fifo && !shift.predecessors.is_empty() {
            return Err("FIFO domain does not accept predecessor edges".to_string());
        }
        if self.seen_signatures.contains_key(&shift.signature) {
            return Ok(());
        }
        if shift.amount == 0 {
            return Err("stateful shift with zero amount".to_string());
        }
        if shift.first_seen_at_ns == 0 {
            shift.first_seen_at_ns = now_ns();
        }
        self.seen_signatures.insert(shift.signature.clone(), ());
        let mut pending = self.pending_stateful.lock().unwrap();
        pending.push(shift);
        Ok(())
    }

    /// Synthesize all pending commutative deltas via NTT and apply stateful
    /// transactions from the DAG in topological order.
    pub fn synthesize(
        &self,
        key_registry: &HashMap<AccountId, ed25519_dalek::VerifyingKey>,
    ) -> SynthesisResult {
        let mut result = SynthesisResult::default();
        let start = Instant::now();
        let finalized_at = now_ns();

        // Increment monotonic synthesis tick at the start of each cycle.
        let tick = self.synthesis_tick.fetch_add(1, Ordering::SeqCst);

        // 0. Metabolic decay: exponentially decay every wave-field balance by
        //    B(t) = B(0) * e^(-λt), using the DEX domain's λ.  Staked operators
        //    are immune (their locked balances back the network).  Of the value
        //    that decays away, a fixed fraction (`METABOLIC_BURN_BP`) is
        //    permanently burned and the remainder is redistributed to operators
        //    and liquidity providers below.
        let immune_accounts: std::collections::HashSet<AccountId> = self
            .stake_table
            .staked_operators()
            .into_iter()
            .map(|(operator, _)| operator)
            .collect();
        let dex_lambda = self
            .domain_registry
            .read()
            .unwrap()
            .get(&crate::crypto::DEFAULT_DEX_DOMAIN)
            .map(|p| p.metabolic_lambda_ppm)
            .unwrap_or(crate::value::metabolic::DEFAULT_DEX_LAMBDA_PPM);
        let decayed = {
            let mut field = self.wave_field.lock().unwrap();
            field.apply_metabolic_decay(tick, dex_lambda, &immune_accounts)
        };
        result.metabolic_burned = decayed;
        // Record the total decayed value into the engine's running total for
        // reporting surfaces (API / persistence).
        self.metabolic_engine.record_burn(decayed);

        // Deterministic integer split: burn a fixed fraction, redistribute the
        // rest.  The remainder (and any rounding) always goes to rewards so no
        // value is lost and every honest node computes the same partition.
        let burn_share = decayed
            .saturating_mul(crate::value::metabolic::METABOLIC_BURN_BP as u128)
            / crate::value::metabolic::BASIS_POINTS_DENOMINATOR as u128;
        let reward_share = decayed - burn_share;
        if burn_share > 0 {
            self.supply_tracker.burn(burn_share);
        }
        if reward_share > 0 {
            let reward_pool = self.reward_pool.read().unwrap();
            reward_pool.distribute(reward_share, &self.stake_table);
        }

        // 0b. Sync decayed wave-field balances into the DAG so that stateful
        //     simulation and double-spend detection operate on the true,
        //     metabolically-decayed available balances.
        //     Lock order: dag first, then wave_field (consistent with the rest
        //     of the oscillator and persistence::save).
        {
            let mut dag = self.dag.lock().unwrap();
            let field = self.wave_field.lock().unwrap();
            for entry in field.accounts.iter() {
                dag.balances
                    .insert(*entry.key(), entry.value().balance.units);
            }
        }

        // 1. Move pending stateful shifts into the DAG.
        let mut finalized_latency_ms = 0.0f64;
        let mut finalized_count = 0usize;
        {
            let mut pending = self.pending_stateful.lock().unwrap();
            let shifts: Vec<StatefulShift> = pending.drain(..).collect();
            drop(pending);

            let mut dag = self.dag.lock().unwrap();
            for shift in shifts {
                let Some(pk) = key_registry.get(&shift.from) else {
                    result
                        .stateful_rejected
                        .push(DagError::InvalidSignature(shift.hash()));
                    continue;
                };
                let depth = self
                    .domain_registry
                    .read()
                    .unwrap()
                    .get(&shift.domain)
                    .map(|p| p.finalization_depth)
                    .unwrap_or(VectorClockDag::FINALIZATION_DEPTH);
                if let Err(e) = dag.insert(shift, pk, tick, depth) {
                    result.stateful_rejected.push(e);
                }
            }

            // Detect and mark double-spend attempts.
            let double_spends = dag.detect_double_spends();
            for err in &double_spends {
                if let DagError::DoubleSpend(hash) = err {
                    if let Some(node) = dag.nodes.get_mut(hash) {
                        node.status = ShiftStatus::Rejected(DagError::DoubleSpend(*hash));
                    }
                }
            }
            result.stateful_rejected.extend(double_spends);

            // Promote accepted shifts to finalized after K subsequent ticks.
            let (promoted, promoted_latency_ms) = dag.promote_to_finalized(tick, finalized_at);
            finalized_count += promoted;
            finalized_latency_ms += promoted_latency_ms;
        }

        // 2. Synthesize commutative batch.
        let mut comm_root = [0u8; 32];
        {
            let mut pending = self.pending_commutative.lock().unwrap();
            if !pending.is_empty() {
                let deltas: Vec<(Coordinate, i128, PoolId)> = pending.drain(..).collect();
                result.commutative_applied = deltas.len();
                comm_root = commutative_root(tick, &deltas);
                let mut field = self.wave_field.lock().unwrap();
                if let Err(e) = field.synthesize_commutative_batch(&deltas) {
                    warn!("commutative synthesis failed: {}", e);
                    pending.extend(deltas);
                    result.commutative_applied = 0;
                    comm_root = [0u8; 32];
                }
            }
        }

        // 3. Apply stateful DAG in topological order.
        let dag = self.dag.lock().unwrap();
        let order = match dag.topological_order() {
            Ok(o) => o,
            Err(e) => {
                result.stateful_rejected.push(e);
                return result;
            }
        };

        // Start from the cumulative DAG balances (already decayed) and apply only
        // shifts that have not yet been applied.  Marking applied shifts prevents
        // them from being replayed on subsequent ticks.  Fees are deducted from
        // the sender and accrue to the reward pool according to the domain's fee
        // policy.
        let mut simulated_balances = dag.balances.clone();
        let mut stateful_hashes = Vec::with_capacity(order.len());
        let mut active_accounts = std::collections::HashSet::new();
        let mut total_fees = 0u128;
        for hash in order {
            let node = dag.nodes.get(&hash).expect("hash in DAG");

            // Skip shifts already rejected by double-spend detection or applied.
            if matches!(node.status, ShiftStatus::Rejected(_)) || node.applied {
                continue;
            }

            let shift = &node.shift;

            let Some(pk) = key_registry.get(&shift.from) else {
                result
                    .stateful_rejected
                    .push(DagError::InvalidSignature(hash));
                continue;
            };
            if !shift.verify_signature(pk) {
                result
                    .stateful_rejected
                    .push(DagError::InvalidSignature(hash));
                continue;
            }

            let (fee, net_amount) = match self.compute_signal_fee(shift) {
                Ok(v) => v,
                Err(_) => {
                    result.stateful_rejected.push(DagError::InsufficientBalance(hash));
                    continue;
                }
            };

            let balance = simulated_balances.get(&shift.from).copied().unwrap_or(0);
            if balance < shift.amount {
                result
                    .stateful_rejected
                    .push(DagError::InsufficientBalance(hash));
                continue;
            }

            *simulated_balances.get_mut(&shift.from).unwrap() -= shift.amount;
            *simulated_balances.entry(shift.to).or_insert(0) += net_amount;
            total_fees = total_fees.saturating_add(fee);
            result.stateful_applied += 1;
            stateful_hashes.push(hash);
            // Record both parties as active so they receive metabolic-decay
            // grace starting next tick.  Self-transfers do not count as real
            // economic activity, otherwise a whale could bypass decay for free by
            // scripting transfers to themselves.
            if shift.from != shift.to {
                active_accounts.insert(shift.from);
                active_accounts.insert(shift.to);
            }
        }
        drop(dag);

        // 3b. Apply verified EVM transactions in nonce order.
        let evm_hashes = {
            let mut evm_pool = self.evm_pool.lock().unwrap();
            let (evm_applied, evm_latency_ms, hashes) = evm_pool.synthesize(
                &mut simulated_balances, finalized_at, tick);
            result.evm_applied = evm_applied;
            finalized_count += evm_applied;
            finalized_latency_ms += evm_latency_ms;
            hashes
        };

        // Deduct accumulated signal fees from the wave-field sender balances and
        // add them to the reward pool.  Compute fee_debt while holding the DAG,
        // then release it before touching wave_field to preserve the global
        // dag-first lock order.
        if total_fees > 0 {
            let mut fee_debt: std::collections::HashMap<AccountId, u128> = std::collections::HashMap::new();
            {
                let dag = self.dag.lock().unwrap();
                for hash in &stateful_hashes {
                    if let Some(node) = dag.nodes.get(hash) {
                        let (fee, _) = self.compute_signal_fee(&node.shift).unwrap_or((0, node.shift.amount));
                        *fee_debt.entry(node.shift.from).or_insert(0) += fee;
                    }
                }
            }
            {
                let field = self.wave_field.lock().unwrap();
                for (account, fee) in fee_debt {
                    field.debit_account(account, fee.min(field.account_balance(account).units));
                }
            }
            {
                let reward_pool = self.reward_pool.read().unwrap();
                reward_pool.distribute_fees(total_fees, &self.stake_table);
            }
        }

        // Commit applied stateful shifts and the cumulative balance set back to
        // the DAG so future ticks do not replay already-settled shifts.
        {
            let mut dag = self.dag.lock().unwrap();
            for hash in &stateful_hashes {
                if let Some(node) = dag.nodes.get_mut(hash) {
                    node.applied = true;
                }
            }
            dag.balances = simulated_balances.clone();
        }

        // Sync wave-field account balances with DAG result.  Because fees were
        // already debited above, only transfer the net simulated balances here
        // so we do not double-charge the sender.
        let field = self.wave_field.lock().unwrap();
        for (account, balance) in &simulated_balances {
            field.ensure_account(*account);
            if let Some(mut state) = field.accounts.get_mut(account) {
                state.balance.units = *balance;
                // Accounts touched by an applied stateful shift this tick start
                // their activity grace window.
                if active_accounts.contains(account) {
                    state.balance.last_active_tick = tick;
                }
            }
        }
        result.final_balances = simulated_balances.clone();

        // 4. Optionally sign a synthesis certificate if the operator is staked.
        if let Some(ref op_kp) = self.operator_keypair {
            if !self.stake_table.is_staked(&op_kp.account_id()) {
                return result;
            }
            let state_root = stateful_root(tick, &stateful_hashes);
            let bal_root = balances_root(&simulated_balances);
            let stake_root = self.stake_table.root();
            let reward_root = self.reward_pool.read().unwrap().root();
            let evm_r = evm_root(tick, &evm_hashes);
            let timestamp_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let cert = SynthesisCertificate::sign(
                op_kp,
                tick,
                result.commutative_applied,
                result.stateful_applied,
                result.evm_applied,
                comm_root,
                state_root,
                bal_root,
                stake_root,
                reward_root,
                evm_r,
                result.metabolic_burned,
                timestamp_ns,
            );
            // Count our own certificate toward local quorum detection.
            let _ = self.ingest_certificate(cert.clone(), key_registry);
            self.certificates.write().unwrap().insert(tick, cert);
        }

        // Compute real performance metrics.
        result.elapsed_ms = start.elapsed().as_nanos() as f64 / 1_000_000.0;
        let total_processed = result.commutative_applied
            + result.stateful_applied
            + result.evm_applied;
        if result.elapsed_ms > 0.0 {
            result.throughput_per_sec = (total_processed as f64) / (result.elapsed_ms / 1000.0);
        }
        if finalized_count > 0 {
            result.avg_latency_ms = finalized_latency_ms / finalized_count as f64;
        }

        result
    }

    pub fn tick_vector_clock(&self) {
        let mut vc = self.vector_clock.lock().unwrap();
        vc.tick(self.id);
    }

    pub fn current_vector_clock(&self) -> VectorClock {
        self.vector_clock.lock().unwrap().clone()
    }

    pub fn stateful_count(&self) -> usize {
        self.dag.lock().unwrap().nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::DEFAULT_DEX_DOMAIN;
    use crate::crypto::keys::KeyPair;

    #[test]
    fn oscillator_aggregates_commutative_and_applies_stateful() {
        let osc = Oscillator::new([1u8; 32], 64);
        let alice = KeyPair::generate();
        let bob = KeyPair::generate();
        osc.seed_account(alice.account_id(), 10_000_000_000_000);

        // Commutative: add 100 to pool every tick.
        let pool = [9u8; 32];
        for i in 0..10 {
            let shift = CommutativeShift::new(
                &alice,
                DEFAULT_DEX_DOMAIN,
                Coordinate::from_scalar(i as u64),
                100,
                pool,
                i as u64,
                0,
            );
            osc.ingest(Signal::Commutative(shift)).unwrap();
        }

        // Stateful: send 500 to Bob.
        let mut vc = VectorClock::new();
        vc.tick(osc.id);
        let st = StatefulShift::new(&alice, DEFAULT_DEX_DOMAIN, bob.account_id(), 500, vc, vec![], 1, 0);
        osc.ingest(Signal::Stateful(st)).unwrap();

        let mut registry = HashMap::new();
        registry.insert(alice.account_id(), alice.public_key());
        let result = osc.synthesize(&registry);

        assert_eq!(result.commutative_applied, 10);
        assert_eq!(result.stateful_applied, 1);
        assert_eq!(result.final_balances[&bob.account_id()], 500);

        let field = osc.wave_field.lock().unwrap();
        assert_eq!(field.pool_balance(pool).units, 10 * 100);
    }
}
