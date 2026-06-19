use crate::crypto::AccountId;
use dashmap::DashMap;

/// Unique identifier for a metabolic stream.
pub type StreamId = [u8; 32];

/// A continuous value stream whose balance degrades over synthesis ticks.
///
/// Decay is driven by the logical synthesis tick, not wall-clock time, so
/// every honest node computes exactly the same burned amount at the same tick.
#[derive(Clone, Debug)]
pub struct MetabolicStream {
    pub id: StreamId,
    pub owner: AccountId,
    /// Burn rate in sub-units per synthesis tick.
    pub rate_per_tick: u128,
    /// Remaining balance in sub-units.
    pub remaining: u128,
    pub last_update_tick: u64,
}

impl MetabolicStream {
    pub fn new(
        id: StreamId,
        owner: AccountId,
        rate_per_tick: u128,
        initial_balance: u128,
    ) -> Self {
        Self {
            id,
            owner,
            rate_per_tick,
            remaining: initial_balance,
            last_update_tick: 0,
        }
    }

    /// Advance the stream by the number of synthesis ticks that have elapsed
    /// since the last update. Returns the burned value and whether the stream
    /// is now exhausted.
    pub fn process(&mut self, tick: u64) -> (u128, bool) {
        let elapsed = tick.saturating_sub(self.last_update_tick);
        let burned = self
            .rate_per_tick
            .saturating_mul(elapsed as u128)
            .min(self.remaining);
        self.remaining = self.remaining.saturating_sub(burned);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::KeyPair;

    #[test]
    fn metabolic_decay_burns_exact_amount_per_tick() {
        let engine = MetabolicDecayEngine::new();
        let owner = KeyPair::generate().account_id();
        let stream = MetabolicStream::new([1u8; 32], owner, 100, 10_000);
        engine.add_stream(stream);

        let burned = engine.process_metabolic_degradation(10);
        assert_eq!(burned, 1_000); // 10 ticks * 100 per tick
        assert_eq!(engine.active_stream_count(), 1);

        let burned = engine.process_metabolic_degradation(20);
        assert_eq!(burned, 1_000); // another 10 ticks * 100
    }

    #[test]
    fn metabolic_decay_removes_exhausted_streams() {
        let engine = MetabolicDecayEngine::new();
        let owner = KeyPair::generate().account_id();
        let stream = MetabolicStream::new([2u8; 32], owner, 1_000, 1_000);
        engine.add_stream(stream);

        engine.process_metabolic_degradation(1);
        assert_eq!(engine.active_stream_count(), 0);
    }

    #[test]
    fn metabolic_decay_is_idempotent_across_repeated_ticks() {
        let engine = MetabolicDecayEngine::new();
        let owner = KeyPair::generate().account_id();
        let stream = MetabolicStream::new([3u8; 32], owner, 50, 1_000);
        engine.add_stream(stream);

        assert_eq!(engine.process_metabolic_degradation(5), 250);
        assert_eq!(engine.process_metabolic_degradation(5), 0);
        assert_eq!(engine.process_metabolic_degradation(20), 750);
    }
}
