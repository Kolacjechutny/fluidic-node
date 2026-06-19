use crate::field::wave_field::WAVE_PRECISION;

/// A continuous value stream specifies a burn rate in sub-units per synthesis
/// tick. The oscillator updates the residual balance as logical ticks advance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueStream {
    /// Total value escrowed for the stream, in sub-units.
    pub total: u128,
    /// Burn rate in sub-units per synthesis tick.
    pub rate_per_tick: u128,
    /// Optional hard cap on stream duration, in ticks.
    pub max_duration_ticks: u64,
}

impl ValueStream {
    pub fn new(wave_amount: u128, rate_per_second_wave: u128, max_duration_ticks: u64) -> Self {
        Self {
            total: wave_amount.saturating_mul(WAVE_PRECISION),
            rate_per_tick: rate_per_second_wave.saturating_mul(WAVE_PRECISION),
            max_duration_ticks,
        }
    }

    pub fn from_subunits(total: u128, rate_per_tick: u128, max_duration_ticks: u64) -> Self {
        Self {
            total,
            rate_per_tick,
            max_duration_ticks,
        }
    }

    /// Compute the amount burned after `elapsed_ticks` logical ticks.
    pub fn burned_in(elapsed_ticks: u64, rate_per_tick: u128) -> u128 {
        rate_per_tick.saturating_mul(elapsed_ticks as u128)
    }

    pub fn duration_ticks(&self) -> u64 {
        if self.rate_per_tick == 0 {
            return 0;
        }
        let ticks = self.total / self.rate_per_tick;
        ticks.min(self.max_duration_ticks as u128) as u64
    }
}

/// Runtime state of a value stream inside an oscillator.
#[derive(Clone, Debug)]
pub struct StreamState {
    pub stream: ValueStream,
    pub remaining: u128,
    pub last_update_tick: u64,
    pub start_tick: u64,
}

impl StreamState {
    pub fn new(stream: ValueStream) -> Self {
        Self {
            remaining: stream.total,
            last_update_tick: 0,
            start_tick: 0,
            stream,
        }
    }

    /// Advance the stream by the elapsed logical ticks and burn the
    /// corresponding amount. Returns the amount burned in this tick.
    pub fn tick(&mut self, tick: u64) -> u128 {
        let elapsed_ticks = tick.saturating_sub(self.last_update_tick);
        let burned = ValueStream::burned_in(elapsed_ticks, self.stream.rate_per_tick);
        let burned = burned.min(self.remaining);
        self.remaining -= burned;
        self.last_update_tick = tick;
        burned
    }

    pub fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_burn_rate() {
        // 1 WAVE per tick, check 5 ticks.
        let stream = ValueStream::new(10, 1, 10);
        let burned = ValueStream::burned_in(5, stream.rate_per_tick);
        assert_eq!(burned, 5 * WAVE_PRECISION);
    }

    #[test]
    fn stream_state_exhausts() {
        let mut state = StreamState::new(ValueStream::from_subunits(
            WAVE_PRECISION,
            WAVE_PRECISION,
            10,
        ));
        assert_eq!(state.tick(1), WAVE_PRECISION);
        assert!(state.is_exhausted());
    }
}
