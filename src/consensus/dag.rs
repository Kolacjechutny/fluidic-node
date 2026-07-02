use crate::crypto::{StatefulShift, TxHash, VectorClock};
use std::collections::{HashMap, HashSet};

/// Lifecycle status of a stateful shift in the DAG.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShiftStatus {
    /// Accepted into the DAG but not yet finalized.
    Accepted,
    /// Rejected during insertion or synthesis.
    Rejected(DagError),
    /// Accepted and survived K subsequent synthesis ticks without conflict.
    Finalized,
}

/// A node in the causal DAG of stateful phase-shifts.
#[derive(Clone, Debug)]
pub struct DagNode {
    pub hash: TxHash,
    pub shift: StatefulShift,
    pub children: HashSet<TxHash>,
    pub inserted_at_tick: u64,
    pub status: ShiftStatus,
    /// Domain-specific finalization depth required before this shift is final.
    pub finalization_depth: u64,
    /// Wall-clock time when the shift was first observed by this node (ns).
    pub first_seen_at_ns: u64,
    /// Synthesis tick at which this node was promoted to finalized.
    pub finalized_at_tick: Option<u64>,
    /// Whether this shift has already been applied to the cumulative balances.
    /// Applied shifts are skipped during synthesis so they are not replayed.
    pub applied: bool,
}

/// Error variants returned by the DAG validator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DagError {
    MissingPredecessor(TxHash),
    InvalidSignature(TxHash),
    InsufficientBalance(TxHash),
    DoubleSpend(TxHash),
    CausalCycle(TxHash),
}

/// Vector-clock DAG that orders state-dependent phase-shifts before they are
/// applied to the wave-field. It also enforces account balance conservation.
pub struct VectorClockDag {
    pub nodes: HashMap<TxHash, DagNode>,
    pub roots: HashSet<TxHash>,
    pub tips: HashMap<crate::crypto::AccountId, TxHash>,
    pub balances: HashMap<crate::crypto::AccountId, u128>,
    /// Shifts that failed DAG insertion, keyed by their hash.
    pub rejected: HashMap<TxHash, DagError>,
    /// Maximum vector-clock value observed across all inserted shifts per node id.
    pub max_clock: VectorClock,
    /// Latest vector-clock observed for each sender account.
    pub account_tips: HashMap<crate::crypto::AccountId, VectorClock>,
}

impl VectorClockDag {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            roots: HashSet::new(),
            tips: HashMap::new(),
            balances: HashMap::new(),
            rejected: HashMap::new(),
            max_clock: VectorClock::new(),
            account_tips: HashMap::new(),
        }
    }

    pub fn seed_balance(&mut self, account: crate::crypto::AccountId, amount: u128) {
        *self.balances.entry(account).or_insert(0) += amount;
    }

    /// Number of synthesis ticks a shift must survive without conflict before it is finalized.
    pub const FINALIZATION_DEPTH: u64 = 3;

    /// Insert a stateful shift into the DAG after validating signature and ancestry.
    pub fn insert(
        &mut self,
        shift: StatefulShift,
        public_key: &ed25519_dalek::VerifyingKey,
        tick: u64,
        finalization_depth: u64,
    ) -> Result<TxHash, DagError> {
        let hash = shift.hash();

        if !shift.verify_signature(public_key) {
            let err = DagError::InvalidSignature(hash);
            self.rejected.insert(hash, err.clone());
            return Err(err);
        }

        if self.nodes.contains_key(&hash) {
            return Ok(hash);
        }

        // All declared predecessors must already exist in the DAG.
        for pred in &shift.predecessors {
            if !self.nodes.contains_key(pred) {
                let err = DagError::MissingPredecessor(*pred);
                self.rejected.insert(hash, err.clone());
                return Err(err);
            }
        }

        let first_seen_at_ns = shift.first_seen_at_ns;
        let node = DagNode {
            hash,
            shift: shift.clone(),
            children: HashSet::new(),
            inserted_at_tick: tick,
            status: ShiftStatus::Accepted,
            finalization_depth,
            first_seen_at_ns,
            finalized_at_tick: None,
            applied: false,
        };

        // Update causal bookkeeping.
        self.max_clock.merge(&shift.vector_clock);
        self.account_tips
            .entry(shift.from)
            .and_modify(|vc| vc.merge(&shift.vector_clock))
            .or_insert_with(|| shift.vector_clock.clone());

        if shift.predecessors.is_empty() {
            self.roots.insert(hash);
        } else {
            for pred in &shift.predecessors {
                self.nodes
                    .get_mut(pred)
                    .expect("predecessor checked above")
                    .children
                    .insert(hash);
            }
        }

        self.nodes.insert(hash, node);

        // Update account tip if this shift advances the sender's vector clock.
        let sender_tip = self.tips.entry(shift.from).or_insert(hash);
        if let Some(existing) = self.nodes.get(sender_tip) {
            if existing
                .shift
                .vector_clock
                .happened_before(&shift.vector_clock)
            {
                *sender_tip = hash;
            }
        } else {
            *sender_tip = hash;
        }

        Ok(hash)
    }

    /// Promote accepted shifts to finalized once they have survived
    /// `FINALIZATION_DEPTH` subsequent synthesis ticks.
    ///
    /// Returns `(promoted_count, total_latency_ms)` for newly finalized nodes,
    /// measured from when each shift was first seen to `finalized_at_ns`.
    pub fn promote_to_finalized(
        &mut self,
        current_tick: u64,
        finalized_at_ns: u64,
    ) -> (usize, f64) {
        let mut promoted = 0usize;
        let mut total_latency_ms = 0.0f64;
        for node in self.nodes.values_mut() {
            if matches!(node.status, ShiftStatus::Accepted)
                && current_tick.saturating_sub(node.inserted_at_tick) >= node.finalization_depth
            {
                node.status = ShiftStatus::Finalized;
                node.finalized_at_tick = Some(current_tick);
                promoted += 1;
                if finalized_at_ns > node.first_seen_at_ns {
                    total_latency_ms +=
                        ((finalized_at_ns - node.first_seen_at_ns) as f64) / 1_000_000.0;
                }
            }
        }
        (promoted, total_latency_ms)
    }

    /// Look up the current status of a shift by hash.
    pub fn shift_status(&self, hash: &TxHash) -> Option<ShiftStatus> {
        if let Some(node) = self.nodes.get(hash) {
            return Some(node.status.clone());
        }
        self.rejected.get(hash).cloned().map(ShiftStatus::Rejected)
    }

    /// Return finalized shifts whose `finalized_at_tick` is at least `from`.
    pub fn finalized_shifts_since(&self, from: u64) -> Vec<&DagNode> {
        self.nodes
            .values()
            .filter(|n| {
                n.status == ShiftStatus::Finalized
                    && n.finalized_at_tick.map(|t| t >= from).unwrap_or(false)
            })
            .collect()
    }

    /// Return the latest observed vector clock for an account, or an empty clock.
    pub fn account_tip(&self, account: &crate::crypto::AccountId) -> VectorClock {
        self.account_tips
            .get(account)
            .cloned()
            .unwrap_or_else(VectorClock::new)
    }

    /// Validate a submitted vector clock or derive a valid next clock for the sender.
    ///
    /// Rules:
    /// - The sender's own entry must be exactly `current_own_time + 1`.
    /// - Every other entry must be <= the maximum time observed for that node id.
    pub fn validate_or_derive(
        &self,
        sender: crate::crypto::AccountId,
        provided: Option<VectorClock>,
    ) -> Result<VectorClock, String> {
        let sender_node = sender.0;
        let own_tip = self.account_tip(&sender).get(&sender_node);
        let expected_own = own_tip + 1;

        if let Some(vc) = provided {
            for (node, time) in &vc.0 {
                if *node == sender_node {
                    if *time != expected_own {
                        return Err(format!(
                            "invalid own vector-clock time: expected {}, got {}",
                            expected_own, time
                        ));
                    }
                } else if *time > self.max_clock.get(node) {
                    return Err(format!(
                        "vector-clock references unseen node {} with time {}",
                        hex::encode(node),
                        time
                    ));
                }
            }
            if !vc.0.contains_key(&sender_node) {
                return Err("vector-clock missing sender's own entry".to_string());
            }
            Ok(vc)
        } else {
            let mut vc = self.account_tip(&sender);
            vc.tick(sender_node);
            Ok(vc)
        }
    }

    /// Strictly validate a vector clock without deriving it.
    pub fn validate_vector_clock(
        &self,
        sender: crate::crypto::AccountId,
        vc: &VectorClock,
    ) -> Result<(), String> {
        self.validate_or_derive(sender, Some(vc.clone())).map(|_| ())
    }

    /// Return a topological ordering of all DAG nodes, starting from roots.
    /// When multiple nodes are ready, prefer the one whose vector clock
    /// happened-before all other ready nodes, matching the whitepaper's causal
    /// precedence rule. Concurrent nodes are ordered deterministically by hash.
    /// Returns an error if a cycle is detected (should be impossible with hash-prefixed refs).
    pub fn topological_order(&self) -> Result<Vec<TxHash>, DagError> {
        let mut in_degree: HashMap<TxHash, usize> = HashMap::new();
        for (h, node) in &self.nodes {
            in_degree.entry(*h).or_insert(0);
            for child in &node.children {
                *in_degree.entry(*child).or_insert(0) += 1;
            }
        }

        let mut ready: Vec<TxHash> = self
            .roots
            .iter()
            .filter(|h| in_degree.get(*h).copied().unwrap_or(0) == 0)
            .copied()
            .collect();
        let mut order = Vec::with_capacity(self.nodes.len());

        while !ready.is_empty() {
            // Pick a ready node whose vector clock causally precedes all other
            // ready nodes. If none exists (all concurrent), fall back to a
            // deterministic hash comparison so every honest node picks the same
            // order.
            let idx = self
                .pick_minimal_ready(&ready)
                .expect("ready set is non-empty");
            let h = ready.swap_remove(idx);

            order.push(h);
            if let Some(node) = self.nodes.get(&h) {
                for child in &node.children {
                    let deg = in_degree.get_mut(child).expect("child in_degree");
                    *deg -= 1;
                    if *deg == 0 {
                        ready.push(*child);
                    }
                }
            }
        }

        if order.len() != self.nodes.len() {
            return Err(DagError::CausalCycle([0u8; 32]));
        }

        Ok(order)
    }

    /// From a non-empty set of ready hashes, return the index of a node whose
    /// vector clock is causally minimal. A node is minimal if no other ready
    /// node happened-before it. If multiple minimal nodes are concurrent, tie
    /// break by lexicographic hash order.
    fn pick_minimal_ready(&self,
        ready: &[TxHash],
    ) -> Option<usize> {
        let mut best_idx = 0usize;
        let mut best_hash = ready.first()?;
        let mut best_vc = &self.nodes.get(best_hash)?.shift.vector_clock;

        for (i, h) in ready.iter().enumerate().skip(1) {
            let vc = &self.nodes.get(h)?.shift.vector_clock;
            if vc.happened_before(best_vc) {
                best_idx = i;
                best_hash = h;
                best_vc = vc;
            } else if vc.concurrent_with(best_vc) {
                // Deterministic tie-break: smaller hash wins.
                if h < best_hash {
                    best_idx = i;
                    best_hash = h;
                    best_vc = vc;
                }
            }
            // If best_vc happened_before vc, keep best.
        }
        Some(best_idx)
    }

    /// Apply stateful transactions in topological order, rejecting any that would
    /// overdraw an account. This enforces the wave-field conservation law.
    pub fn apply_ordered(&self) -> Result<HashMap<crate::crypto::AccountId, u128>, Vec<DagError>> {
        let order = self.topological_order().map_err(|e| vec![e])?;
        let mut simulated = self.balances.clone();
        let mut errors = Vec::new();

        for hash in order {
            let node = self.nodes.get(&hash).expect("hash in DAG");
            let shift = &node.shift;
            let sender_balance = simulated.get(&shift.from).copied().unwrap_or(0);
            if sender_balance < shift.amount {
                errors.push(DagError::InsufficientBalance(hash));
                continue;
            }
            *simulated.get_mut(&shift.from).unwrap() -= shift.amount;
            *simulated.entry(shift.to).or_insert(0) += shift.amount;
        }

        if errors.is_empty() {
            Ok(simulated)
        } else {
            Err(errors)
        }
    }

    /// Detect double-spend attempts: two stateful shifts from the same sender that
    /// are concurrent (neither causally precedes the other) and whose combined
    /// amount exceeds the current balance.
    pub fn detect_double_spends(&self) -> Vec<DagError> {
        let mut by_sender: HashMap<crate::crypto::AccountId, Vec<TxHash>> = HashMap::new();
        for (h, node) in &self.nodes {
            by_sender.entry(node.shift.from).or_default().push(*h);
        }

        let mut violations = Vec::new();
        for (account, mut hashes) in by_sender {
            // Deterministic ordering: earlier inserted shifts win ties.
            hashes.sort_by_key(|h| {
                let node = &self.nodes[h];
                (node.inserted_at_tick, *h)
            });

            let balance = self.balances.get(&account).copied().unwrap_or(0);
            // Naive O(n^2) pairwise check; acceptable for prototype scale.
            for (i, &h1) in hashes.iter().enumerate() {
                let n1 = &self.nodes[&h1];
                let mut spent = n1.shift.amount;
                for &h2 in hashes.iter().skip(i + 1) {
                    let n2 = &self.nodes[&h2];
                    if n1
                        .shift
                        .vector_clock
                        .concurrent_with(&n2.shift.vector_clock)
                    {
                        if spent + n2.shift.amount > balance {
                            violations.push(DagError::DoubleSpend(h2));
                        }
                        spent += n2.shift.amount;
                    }
                }
            }
        }
        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{VectorClock, DEFAULT_DEX_DOMAIN};
    use crate::crypto::keys::KeyPair;

    #[test]
    fn dag_orders_chain() {
        let mut dag = VectorClockDag::new();
        let kp = KeyPair::generate();
        let to = KeyPair::generate().account_id();
        dag.seed_balance(kp.account_id(), 1_000_000_000_000);

        let mut vc = VectorClock::new();
        vc.tick([1u8; 32]);
        let shift1 = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, to, 100, vc.clone(), vec![], 1, 0);
        let h1 = shift1.hash();
        dag.insert(shift1.clone(), &kp.public_key(), 0, 3).unwrap();

        let mut vc2 = vc.clone();
        vc2.tick([1u8; 32]);
        let shift2 = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, to, 200, vc2, vec![h1], 2, 0);
        dag.insert(shift2, &kp.public_key(), 1, 3).unwrap();

        let order = dag.topological_order().unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], h1);

        let balances = dag.apply_ordered().unwrap();
        assert_eq!(balances[&kp.account_id()], 1_000_000_000_000 - 300);
        assert_eq!(balances[&to], 300);
    }

    #[test]
    fn topological_order_uses_vector_clock_tiebreaker() {
        let mut dag = VectorClockDag::new();
        let kp = KeyPair::generate();
        let to = KeyPair::generate().account_id();
        dag.seed_balance(kp.account_id(), 1_000_000_000_000);

        // Two concurrent roots with no predecessor relationship.
        // shift_a has vector clock {node_a: 1}, shift_b has {node_b: 1, node_a: 1}.
        // shift_a happened-before shift_b, so it should be ordered first even if
        // shift_b was inserted first.
        let mut vc_a = VectorClock::new();
        vc_a.tick([1u8; 32]);
        let shift_a = StatefulShift::new(
            &kp,
            DEFAULT_DEX_DOMAIN,
            to,
            100,
            vc_a,
            vec![],
            1,
            0,
        );

        let mut vc_b = VectorClock::new();
        vc_b.tick([2u8; 32]);
        vc_b.tick([1u8; 32]);
        let shift_b = StatefulShift::new(
            &kp,
            DEFAULT_DEX_DOMAIN,
            to,
            100,
            vc_b,
            vec![],
            2,
            0,
        );

        // Insert b first, then a.
        let hash_a = shift_a.hash();
        let hash_b = shift_b.hash();
        dag.insert(shift_b, &kp.public_key(), 0, 3).unwrap();
        dag.insert(shift_a, &kp.public_key(), 0, 3).unwrap();

        let order = dag.topological_order().unwrap();
        assert_eq!(order.len(), 2);
        // a causally precedes b, so a must come first.
        assert_eq!(order[0], hash_a);
        assert_eq!(order[1], hash_b);
    }

    #[test]
    fn dag_detects_double_spend() {
        let mut dag = VectorClockDag::new();
        let kp = KeyPair::generate();
        let to = KeyPair::generate().account_id();
        dag.seed_balance(kp.account_id(), 1_000_000_000_000);

        // Two concurrent transfers from the same account, total > balance.
        let mut vc_a = VectorClock::new();
        vc_a.tick([1u8; 32]);
        let shift_a = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, to, 700_000_000_000, vc_a, vec![], 1, 0);

        let mut vc_b = VectorClock::new();
        vc_b.tick([2u8; 32]);
        let shift_b = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, to, 400_000_000_000, vc_b, vec![], 2, 0);

        dag.insert(shift_a, &kp.public_key(), 0, 3).unwrap();
        dag.insert(shift_b, &kp.public_key(), 0, 3).unwrap();

        let ds = dag.detect_double_spends();
        assert!(!ds.is_empty());
    }

    #[test]
    fn validates_vector_clock_monotonicity() {
        let mut dag = VectorClockDag::new();
        let kp = KeyPair::generate();
        let to = KeyPair::generate().account_id();
        dag.seed_balance(kp.account_id(), 1_000_000_000_000);

        let sender = kp.account_id();
        let sender_node = sender.0;

        // First shift: own clock = 1 is valid.
        let mut vc = VectorClock::new();
        vc.tick(sender_node);
        let shift1 = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, to, 100, vc.clone(), vec![], 1, 0);
        dag.insert(shift1, &kp.public_key(), 0, 3).unwrap();

        // Reject a clock that skips the sender's own tick.
        let mut bad_vc = VectorClock::new();
        bad_vc.tick(sender_node);
        bad_vc.tick(sender_node);
        bad_vc.tick(sender_node);
        assert!(dag.validate_vector_clock(sender, &bad_vc).is_err());

        // Accept the correct successor.
        let mut good_vc = VectorClock::new();
        good_vc.tick(sender_node);
        good_vc.tick(sender_node);
        assert!(dag.validate_vector_clock(sender, &good_vc).is_ok());
    }

    #[test]
    fn derives_vector_clock_when_missing() {
        let mut dag = VectorClockDag::new();
        let kp = KeyPair::generate();
        let sender = kp.account_id();
        dag.seed_balance(sender, 1_000_000_000_000);

        let mut vc = VectorClock::new();
        vc.tick(sender.0);
        let shift = StatefulShift::new(&kp, DEFAULT_DEX_DOMAIN, sender, 1, vc, vec![], 1, 0);
        dag.insert(shift, &kp.public_key(), 0, 3).unwrap();

        let derived = dag.validate_or_derive(sender, None).unwrap();
        assert_eq!(derived.get(&sender.0), 2);
    }
}
