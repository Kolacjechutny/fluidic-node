use crate::crypto::AccountId;
use dashmap::DashMap;

/// Unique identifier for a metabolic stream.
pub type StreamId = [u8; 32];

/// Basis-points denominator (10_000 = 100%).
pub const BASIS_POINTS_DENOMINATOR: u64 = 10_000;

/// Parts-per-million denominator (1_000_000 = 100%) used for the metabolic
/// decay constant λ.  The finer resolution (versus basis points) lets the
/// per-tick decay rate be tuned far more gently than 1 bp = 0.01%/tick allowed.
pub const DECAY_DENOMINATOR: u64 = 1_000_000;

/// Default exponential decay constant λ for the built-in DEX domain, expressed
/// in parts-per-million per synthesis tick.  A value of `20` means each tick a
/// balance retains `(1_000_000 - 20) / 1_000_000 = 99.998%` of its value, i.e.
/// 0.002% decays away per tick.
pub const DEFAULT_DEX_LAMBDA_PPM: u64 = 20;

/// Number of ticks of activity grace: an account that transacted within this
/// many ticks of the current synthesis tick is exempt from metabolic decay.
/// At ~1 tick/second this is a ~4 hour window.
pub const METABOLIC_IDLE_GRACE_TICKS: u64 = 4 * 60 * 60;

/// Fraction of each tick's metabolically-decayed value that is *permanently
/// burned* (removed from supply) rather than redistributed.  Expressed in basis
/// points.  `2_500` = 25%: a quarter of decay is a deflationary sink, the
/// remaining 75% is redistributed to operators and liquidity providers.
pub const METABOLIC_BURN_BP: u64 = 2_500;

/// Integer exponentiation by squaring for `u128`.
///
/// Uses `saturating_mul` so the result is always defined and identical across
/// all honest nodes even on overflow (consensus-critical determinism).  Callers
/// must keep `base^exp` within `u128` range to obtain mathematically exact
/// decay; see [`decayed_balance`].
pub fn pow(mut base: u128, mut exp: u64) -> u128 {
    let mut acc: u128 = 1;
    while exp > 0 {
        if exp & 1 == 1 {
            acc = acc.saturating_mul(base);
        }
        exp >>= 1;
        if exp > 0 {
            base = base.saturating_mul(base);
        }
    }
    acc
}

/// Closed-form exponential decay of a balance over `elapsed` synthesis ticks:
///
/// ```text
/// B(elapsed) = B(0) * (1_000_000 - λ)^elapsed / 1_000_000^elapsed
/// ```
///
/// This is the discrete integer analogue of `B(t) = B(0) * e^(-λt)`.  All
/// arithmetic is integer-only and deterministic, so every honest node computes
/// the exact same remaining balance for a given `(balance, λ, elapsed)`.
pub fn decayed_balance(balance: u128, rate_ppm: u64, elapsed: u64) -> u128 {
    if balance == 0 || elapsed == 0 {
        return balance;
    }
    // Cap λ strictly below the denominator so a balance never fully vanishes in
    // a single tick and the retained fraction is always >= 1 / 1_000_000.
    let rate_ppm = rate_ppm.min(DECAY_DENOMINATOR - 1);
    let retain = (DECAY_DENOMINATOR - rate_ppm) as u128;
    let numerator = pow(retain, elapsed);
    let denominator = pow(DECAY_DENOMINATOR as u128, elapsed);
    balance.saturating_mul(numerator) / denominator
}

/// A continuous value stream whose balance decays exponentially over synthesis
/// ticks following `B(t) = B(0) * e^(-λt)`.
///
/// Decay is driven by the logical synthesis tick, not wall-clock time, so every
/// honest node computes exactly the same remaining balance at the same tick.
#[derive(Clone, Debug)]
pub struct MetabolicStream {
    pub id: StreamId,
    pub owner: AccountId,
    /// Balance at `created_tick`, the anchor for the closed-form decay curve.
    pub initial_balance: u128,
    /// Synthesis tick at which the stream was created (decay anchor t=0).
    pub created_tick: u64,
    /// Per-domain decay constant λ in parts-per-million per tick (capped < 1_000_000).
    pub rate_ppm: u64,
    /// Remaining balance after the most recent `process` call.
    pub remaining: u128,
    /// Last synthesis tick at which `process` advanced the stream.
    pub last_update_tick: u64,
}

impl MetabolicStream {
    /// Create a stream anchored at tick 0 with the given decay constant.
    pub fn new(id: StreamId, owner: AccountId, initial_balance: u128, rate_ppm: u64) -> Self {
        Self::new_at(id, owner, initial_balance, rate_ppm, 0)
    }

    /// Create a stream anchored at an explicit `created_tick`.
    pub fn new_at(
        id: StreamId,
        owner: AccountId,
        initial_balance: u128,
        rate_ppm: u64,
        created_tick: u64,
    ) -> Self {
        let rate_ppm = rate_ppm.min(DECAY_DENOMINATOR - 1);
        Self {
            id,
            owner,
            initial_balance,
            created_tick,
            rate_ppm,
            remaining: initial_balance,
            last_update_tick: created_tick,
        }
    }

    /// Remaining balance at an absolute synthesis `tick`, computed from the
    /// closed-form exponential curve anchored at `created_tick`.
    pub fn remaining_at(&self, tick: u64) -> u128 {
        let elapsed = tick.saturating_sub(self.created_tick);
        decayed_balance(self.initial_balance, self.rate_ppm, elapsed)
    }

    /// Advance the stream to absolute synthesis `tick`.  Returns the value
    /// burned since the previous `process` call and whether the stream is now
    /// fully exhausted (remaining == 0).
    pub fn process(&mut self, tick: u64) -> (u128, bool) {
        let new_remaining = self.remaining_at(tick).min(self.remaining);
        let burned = self.remaining.saturating_sub(new_remaining);
        self.remaining = new_remaining;
        self.last_update_tick = tick;
        (burned, self.remaining == 0)
    }
}

/// Engine that owns all active metabolic streams and processes their decay
/// in a single pass over the oscillator's execution loop.
#[derive(Debug, Default)]
pub struct MetabolicDecayEngine {
    pub streams: DashMap<StreamId, MetabolicStream>,
    pub total_burned: std::sync::Mutex<u128>,
}

impl MetabolicDecayEngine {
    pub fn new() -> Self {
        Self {
            streams: DashMap::new(),
            total_burned: std::sync::Mutex::new(0),
        }
    }

    pub fn add_stream(&self, stream: MetabolicStream) {
        self.streams.insert(stream.id, stream);
    }

    /// Process every active stream once, burn the deterministic tick-based
    /// amount, remove exhausted streams, and return the total burned in tick.
    pub fn process_metabolic_degradation(&self, tick: u64) -> u128 {
        let mut tick_burn = 0u128;
        self.streams.retain(|_id, stream| {
            let (burned, exhausted) = stream.process(tick);
            tick_burn = tick_burn.saturating_add(burned);
            !exhausted
        });
        *self.total_burned.lock().unwrap() += tick_burn;
        tick_burn
    }

    pub fn active_stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn total_burned(&self) -> u128 {
        *self.total_burned.lock().unwrap()
    }

    /// Record externally-computed burn (e.g. wave-field decay) into the running
    /// total so reporting surfaces (API, persistence) stay accurate.
    pub fn record_burn(&self, amount: u128) {
        *self.total_burned.lock().unwrap() += amount;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::KeyPair;

    #[test]
    fn pow_uses_exponentiation_by_squaring() {
        assert_eq!(pow(1, 0), 1);
        assert_eq!(pow(7, 1), 7);
        assert_eq!(pow(2, 10), 1024);
        assert_eq!(pow(9_999, 2), 99_980_001);
        assert_eq!(pow(10_000, 3), 1_000_000_000_000);
    }

    #[test]
    fn decayed_balance_matches_closed_form() {
        // λ = 10_000 ppm (1% per tick), retain 990_000/1_000_000.
        let initial = 1_000_000u128;
        assert_eq!(decayed_balance(initial, 10_000, 0), initial);
        assert_eq!(decayed_balance(initial, 10_000, 1), 990_000);
        // 1_000_000 * 990_000^2 / 1_000_000^2
        assert_eq!(decayed_balance(initial, 10_000, 2), 980_100);
        // 1_000_000 * 990_000^3 / 1_000_000^3
        assert_eq!(decayed_balance(initial, 10_000, 3), 970_299);
    }

    #[test]
    fn metabolic_burn_split_is_deterministic_25_75() {
        // Helper mirrors the integer split applied in the synthesis loop.
        let split = |decayed: u128| -> (u128, u128) {
            let burn = decayed.saturating_mul(METABOLIC_BURN_BP as u128)
                / BASIS_POINTS_DENOMINATOR as u128;
            (burn, decayed - burn)
        };
        // Exact quarter.
        assert_eq!(split(1_000_000), (250_000, 750_000));
        // Rounding always favors the reward share; nothing is lost.
        let (burn, reward) = split(3);
        assert_eq!(burn, 0); // 3 * 2500 / 10000 = 0
        assert_eq!(burn + reward, 3);
        let (burn, reward) = split(4);
        assert_eq!(burn, 1); // 4 * 2500 / 10000 = 1
        assert_eq!(burn + reward, 4);
        // Conservation for an arbitrary value.
        let (burn, reward) = split(987_654_321);
        assert_eq!(burn + reward, 987_654_321);
    }

    #[test]
    fn lambda_is_capped_below_denominator() {
        // λ >= 1_000_000 is clamped to 999_999 so at least 1/1_000_000 always survives.
        let owner = KeyPair::generate().account_id();
        let stream = MetabolicStream::new([9u8; 32], owner, 1_000_000, 2_000_000);
        assert_eq!(stream.rate_ppm, DECAY_DENOMINATOR - 1);
        // One tick at the capped rate retains 1/1_000_000 of the balance.
        assert_eq!(stream.remaining_at(1), 1);
    }

    #[test]
    fn stream_remaining_follows_exponential_curve() {
        let owner = KeyPair::generate().account_id();
        let stream = MetabolicStream::new([1u8; 32], owner, 1_000_000, 10_000);
        assert_eq!(stream.remaining_at(0), 1_000_000);
        assert_eq!(stream.remaining_at(1), 990_000);
        assert_eq!(stream.remaining_at(2), 980_100);
        assert_eq!(stream.remaining_at(5), decayed_balance(1_000_000, 10_000, 5));
    }

    #[test]
    fn process_returns_incremental_burn_each_tick() {
        let owner = KeyPair::generate().account_id();
        let mut stream = MetabolicStream::new([2u8; 32], owner, 1_000_000, 10_000);

        let (burned, exhausted) = stream.process(1);
        assert_eq!(burned, 10_000); // 1_000_000 - 990_000
        assert!(!exhausted);
        assert_eq!(stream.remaining, 990_000);

        let (burned, exhausted) = stream.process(2);
        assert_eq!(burned, 9_900); // 990_000 - 980_100
        assert!(!exhausted);
        assert_eq!(stream.remaining, 980_100);

        // Re-processing the same tick burns nothing (idempotent).
        let (burned, _) = stream.process(2);
        assert_eq!(burned, 0);
    }

    #[test]
    fn engine_accumulates_exponential_burn() {
        let engine = MetabolicDecayEngine::new();
        let owner = KeyPair::generate().account_id();
        engine.add_stream(MetabolicStream::new([3u8; 32], owner, 1_000_000, 10_000));

        let burned = engine.process_metabolic_degradation(1);
        assert_eq!(burned, 10_000);
        assert_eq!(engine.total_burned(), 10_000);

        let burned = engine.process_metabolic_degradation(2);
        assert_eq!(burned, 9_900);
        assert_eq!(engine.total_burned(), 19_900);
        assert_eq!(engine.active_stream_count(), 1);
    }

    #[test]
    fn engine_removes_fully_exhausted_streams() {
        let engine = MetabolicDecayEngine::new();
        let owner = KeyPair::generate().account_id();
        // A tiny balance with the maximal capped rate decays to zero quickly.
        engine.add_stream(MetabolicStream::new([4u8; 32], owner, 1, 9_999));
        let burned = engine.process_metabolic_degradation(1);
        assert_eq!(burned, 1);
        assert_eq!(engine.active_stream_count(), 0);
    }
}
