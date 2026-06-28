use crate::consensus::{Oscillator, ShiftStatus, SynthesisResult};
use crate::crypto::{
    AccountId, KeyPair, RegistrationShift, Signal, StakeShift, StatefulShift, VectorClock,
};
use ed25519_dalek::VerifyingKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{broadcast, mpsc};

/// Derive a deterministic account for a given base and domain salt.
pub fn derive_account(base: AccountId, salt: &[u8]) -> AccountId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:derived-account:v1");
    hasher.update(&base.0);
    hasher.update(salt);
    AccountId(hasher.finalize().into())
}

/// Snapshot broadcast to WebSocket clients.
#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub wave_reserve: u128,
    pub usdc_reserve: u128,
    pub price: f64,
    /// Signals processed per second during the last synthesis cycle.
    pub throughput: f64,
    /// Average latency (ms) from first seen to finalized for stateful + EVM signals.
    pub latency_ms: f64,
    /// Estimated network round-trip latency (ms) between peers.
    pub network_ms: f64,
    pub metabolic_burned: u128,
    pub commutative_applied: usize,
    pub stateful_applied: usize,
    pub evm_applied: usize,
    pub accounts: HashMap<String, u128>,
}

#[derive(Clone, Copy, Default)]
pub struct SynthesisStats {
    pub commutative_applied: usize,
    pub stateful_applied: usize,
    pub evm_applied: usize,
    pub avg_latency_ms: f64,
    pub throughput_per_sec: f64,
    pub network_ms: f64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RecentShift {
    pub hash: String,
    pub kind: String,
    pub status: String,
    pub domain: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub amount: Option<String>,
    pub timestamp_ns: u64,
}

pub struct ApiState {
    pub oscillator: Arc<Oscillator>,
    pub registry: Arc<RwLock<HashMap<AccountId, VerifyingKey>>>,
    /// Maps a derived token account back to its owner main account.
    pub derived_to_main: Arc<RwLock<HashMap<AccountId, AccountId>>>,
    pub pool_keypair: KeyPair,
    pub pool_wave_account: AccountId,
    pub pool_usdc_account: AccountId,
    pub ws_tx: broadcast::Sender<StateSnapshot>,
    pub stats: Arc<Mutex<SynthesisStats>>,
    /// Optional outbound gossip channel for broadcasting registrations.
    pub gossip: Arc<Mutex<Option<mpsc::Sender<Signal>>>>,
    /// Optional local operator keypair exposed via the operator API.
    pub operator_keypair: Mutex<Option<KeyPair>>,
    /// Recently submitted shifts for the explorer.
    pub recent_shifts: Arc<Mutex<Vec<RecentShift>>>,
}

impl ApiState {
    pub fn new(oscillator: Arc<Oscillator>) -> Self {
        // Use a deterministic pool keypair across all nodes so every mesh member
        // shares the same DEX reserves and account roots.
        let pool_keypair = KeyPair::from_seed(&[0u8; 32]);
        let pool_account = pool_keypair.account_id();
        let pool_wave_account = derive_account(pool_account, b"WAVE");
        let pool_usdc_account = derive_account(pool_account, b"USDC");
        let (ws_tx, _ws_rx) = broadcast::channel(64);

        let state = Self {
            oscillator,
            registry: Arc::new(RwLock::new(HashMap::new())),
            derived_to_main: Arc::new(RwLock::new(HashMap::new())),
            pool_keypair,
            pool_wave_account,
            pool_usdc_account,
            ws_tx,
            stats: Arc::new(Mutex::new(SynthesisStats::default())),
            gossip: Arc::new(Mutex::new(None)),
            operator_keypair: Mutex::new(None),
            recent_shifts: Arc::new(Mutex::new(Vec::with_capacity(200))),
        };

        // Seed the DEX pool.
        state.oscillator.seed_account(pool_wave_account, 100_000_000_000_000_000); // 100k WAVE
        state.oscillator.seed_account(pool_usdc_account, 100_000_000_000_000_000); // 100k USDC

        // Register pool accounts so their signed shifts verify in the DAG.
        state.register_key(pool_wave_account, state.pool_keypair.public_key());
        state.register_key(pool_usdc_account, state.pool_keypair.public_key());

        state
    }

    pub fn set_gossip(&self, sender: mpsc::Sender<Signal>) {
        *self.gossip.lock().unwrap() = Some(sender);
    }

    pub fn set_operator_keypair(&self, keypair: KeyPair) {
        *self.operator_keypair.lock().unwrap() = Some(keypair);
    }

    pub fn broadcast_stake(&self, stake: StakeShift) {
        if let Some(sender) = self.gossip.lock().unwrap().as_ref() {
            let sender = sender.clone();
            let signal = Signal::Stake(stake);
            tokio::spawn(async move {
                if let Err(e) = sender.send(signal).await {
                    tracing::warn!("failed to broadcast stake: {}", e);
                }
            });
        }
    }

    pub fn broadcast_registration(&self, reg: RegistrationShift) {
        if let Some(sender) = self.gossip.lock().unwrap().as_ref() {
            let sender = sender.clone();
            let signal = Signal::Registration(reg);
            tokio::spawn(async move {
                if let Err(e) = sender.send(signal).await {
                    tracing::warn!("failed to broadcast registration: {}", e);
                }
            });
        }
    }

    pub fn register_key(&self, account: AccountId, key: VerifyingKey) {
        self.registry.write().unwrap().insert(account, key);
    }

    pub fn register_derived(&self, derived: AccountId, main: AccountId) {
        self.derived_to_main.write().unwrap().insert(derived, main);
    }

    pub fn main_account(&self, derived: AccountId) -> Option<AccountId> {
        self.derived_to_main.read().unwrap().get(&derived).copied()
    }

    pub fn key_registry(&self) -> HashMap<AccountId, VerifyingKey> {
        self.registry.read().unwrap().clone()
    }

    pub fn record_synthesis(&self, result: &SynthesisResult) {
        let mut stats = self.stats.lock().unwrap();
        stats.commutative_applied += result.commutative_applied;
        stats.stateful_applied += result.stateful_applied;
        stats.evm_applied += result.evm_applied;
        stats.avg_latency_ms = result.avg_latency_ms;
        stats.throughput_per_sec = result.throughput_per_sec;
    }

    pub fn record_shift(&self, shift: RecentShift) {
        let mut buf = self.recent_shifts.lock().unwrap();
        buf.insert(0, shift);
        if buf.len() > 200 {
            buf.truncate(200);
        }
    }

    pub fn token_accounts(&self, user_account: AccountId) -> (AccountId, AccountId) {
        (derive_account(user_account, b"WAVE"), derive_account(user_account, b"USDC"))
    }

    /// Fixed-point price used only for display.  Consensus-critical payouts
    /// should use `wave_to_usdc_out` / `usdc_to_wave_out` to avoid `f64`
    /// precision loss.
    pub fn pool_price(&self) -> f64 {
        let field = self.oscillator.wave_field.lock().unwrap();
        let wave = field.account_balance(self.pool_wave_account).units;
        let usdc = field.account_balance(self.pool_usdc_account).units;
        drop(field);
        if wave == 0 {
            return 0.0;
        }
        usdc as f64 / wave as f64
    }

    /// Integer payout for swapping `wave_in` into the pool, returning USDC.
    pub fn wave_to_usdc_out(&self, wave_in: u128) -> u128 {
        let field = self.oscillator.wave_field.lock().unwrap();
        let wave = field.account_balance(self.pool_wave_account).units;
        let usdc = field.account_balance(self.pool_usdc_account).units;
        drop(field);
        if wave == 0 {
            return 0;
        }
        wave_in.saturating_mul(usdc) / wave
    }

    /// Integer payout for swapping `usdc_in` into the pool, returning WAVE.
    pub fn usdc_to_wave_out(&self, usdc_in: u128) -> u128 {
        let field = self.oscillator.wave_field.lock().unwrap();
        let wave = field.account_balance(self.pool_wave_account).units;
        let usdc = field.account_balance(self.pool_usdc_account).units;
        drop(field);
        if usdc == 0 {
            return 0;
        }
        usdc_in.saturating_mul(wave) / usdc
    }

    pub fn shift_status(&self, hash: &[u8; 32]) -> Option<ShiftStatus> {
        self.oscillator.dag.lock().unwrap().shift_status(hash)
    }

    pub fn snapshot(&self) -> StateSnapshot {
        let (wave_reserve, usdc_reserve) = {
            let field = self.oscillator.wave_field.lock().unwrap();
            (
                field.account_balance(self.pool_wave_account).units,
                field.account_balance(self.pool_usdc_account).units,
            )
        };

        let price = if wave_reserve == 0 {
            0.0
        } else {
            usdc_reserve as f64 / wave_reserve as f64
        };

        let stats = *self.stats.lock().unwrap();

        StateSnapshot {
            wave_reserve,
            usdc_reserve,
            price,
            throughput: stats.throughput_per_sec,
            latency_ms: stats.avg_latency_ms,
            network_ms: 0.0, // populated by the WebSocket task once gossip probes exist
            metabolic_burned: *self.oscillator.metabolic_engine.total_burned.lock().unwrap(),
            commutative_applied: stats.commutative_applied,
            stateful_applied: stats.stateful_applied,
            evm_applied: stats.evm_applied,
            accounts: HashMap::new(),
        }
    }

    /// Update the network latency estimate (called by gossip probe tasks).
    pub fn record_network_latency_ms(&self, ms: f64) {
        let mut stats = self.stats.lock().unwrap();
        // Simple exponential moving average with alpha = 0.2.
        if stats.network_ms == 0.0 {
            stats.network_ms = ms;
        } else {
            stats.network_ms = stats.network_ms * 0.8 + ms * 0.2;
        }
    }
}

pub fn build_pool_payout_shift(
    pool_keypair: &KeyPair,
    from: AccountId,
    to: AccountId,
    amount: u128,
    nonce: u64,
    tip: &VectorClock,
) -> StatefulShift {
    let mut vc = tip.clone();
    vc.tick(from.0);
    let mut shift = StatefulShift {
        domain: crate::crypto::DEFAULT_DEX_DOMAIN,
        from,
        to,
        amount,
        vector_clock: vc,
        predecessors: vec![],
        nonce,
        timestamp_ns: 0,
        first_seen_at_ns: 0,
        signature: vec![],
    };
    let sig = pool_keypair.sign(&shift.signing_bytes());
    shift.signature = sig.to_bytes().to_vec();
    shift
}
