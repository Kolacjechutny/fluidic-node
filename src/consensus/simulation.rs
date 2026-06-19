use crate::consensus::ntt::NttEngine;
use crate::crypto::{CommutativeShift, KeyPair, PoolId, DEFAULT_DEX_DOMAIN};
use crate::field::coordinates::Coordinate;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// In-process mesh simulation used to stress-test wave-field synthesis.
pub struct TuningForkMeshSimulation {
    pub engine: Arc<NttEngine>,
    pub pool_id: PoolId,
    pub target_coordinate: Coordinate,
}

impl TuningForkMeshSimulation {
    pub fn new(ntt_size: usize) -> Self {
        assert!(
            ntt_size.is_power_of_two(),
            "NTT size must be a power of two"
        );
        Self {
            engine: Arc::new(NttEngine::new(ntt_size)),
            pool_id: [0xFF; 32],
            target_coordinate: Coordinate::from_scalar(0),
        }
    }

    /// Fire `count` concurrent commutative updates to the same localized
    /// coordinate/spectrum bin from `threads` parallel workers.
    ///
    /// Returns `(algebraic_total, recovered_bin_value)` so the caller can
    /// assert exact equality. Because the NTT uses integer modular arithmetic,
    /// there is no floating-point drift.
    pub fn stress_bin(&self, count: usize, threads: usize) -> (i64, i64) {
        let bin = self.target_coordinate.to_ntt_index(self.engine.size);
        let totals: Vec<AtomicI64> = (0..self.engine.size).map(|_| AtomicI64::new(0)).collect();

        // Each thread signs and emits its own deltas to avoid contention on
        // a single keypair while still targeting the same coordinate bin.
        let per_thread = count / threads;
        let remainder = count % threads;

        (0..threads).into_par_iter().for_each(|t| {
            let kp = KeyPair::generate();
            let start = t * per_thread;
            let extra = if t == threads - 1 { remainder } else { 0 };
            let mut local = 0i64;
            for i in 0..(per_thread + extra) {
                // Deltas alternate sign to create destructive/constructive
                // interference within the same bin.
                let delta = if (start + i) % 2 == 0 { 17 } else { -7 };
                let _shift = CommutativeShift::new(
                    &kp,
                    DEFAULT_DEX_DOMAIN,
                    self.target_coordinate,
                    delta as i128,
                    self.pool_id,
                    (start + i) as u64,
                    0,
                );
                local += delta as i64;
            }
            totals[bin].fetch_add(local, Ordering::Relaxed);
        });

        let algebraic_total = totals[bin].load(Ordering::Relaxed);

        // Build the time-domain signal from the per-bin atomic totals.
        let mut signal: Vec<u64> = totals
            .iter()
            .map(|a| signed_to_field(a.load(Ordering::Relaxed) as i128))
            .collect();

        // Forward NTT: signal -> frequency domain.
        self.engine.ntt(&mut signal);
        // Inverse NTT: frequency domain -> recovered signal.
        let mut recovered = signal.clone();
        self.engine.intt(&mut recovered);

        let recovered_bin = field_to_signed(recovered[bin]);
        (algebraic_total, recovered_bin)
    }
}

fn signed_to_field(x: i128) -> u64 {
    let p = crate::consensus::ntt::NTT_MODULUS as i128;
    let mut r = x % p;
    if r < 0 {
        r += p;
    }
    r as u64
}

fn field_to_signed(x: u64) -> i64 {
    let p = crate::consensus::ntt::NTT_MODULUS as i64;
    let half = p / 2;
    let v = x as i64;
    if v > half { v - p } else { v }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stress_bin_exact_recovery() {
        let sim = TuningForkMeshSimulation::new(2048);
        let (algebraic, recovered) = sim.stress_bin(100_000, 16);
        assert_eq!(
            algebraic, recovered,
            "NTT/INTT round-trip failed: algebraic={}, recovered={}",
            algebraic, recovered
        );
    }
}
