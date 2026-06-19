use crate::crypto::AccountId;
use std::collections::HashMap;

/// A lease on a contiguous range of frequency bins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BandLease {
    pub owner: AccountId,
    pub start_bin: usize,
    pub end_bin: usize, // exclusive
    pub staked_amount: u128,
    pub throughput_quota: u64,
    pub used_throughput: u64,
}

impl BandLease {
    pub fn bin_count(&self) -> usize {
        self.end_bin.saturating_sub(self.start_bin)
    }

    pub fn has_quota(&self) -> bool {
        self.used_throughput < self.throughput_quota
    }

    pub fn consume_quota(&mut self) -> bool {
        if self.has_quota() {
            self.used_throughput += 1;
            true
        } else {
            false
        }
    }
}

/// Spectrum allocation engine. Applications stake WAVE to secure clear,
/// low-noise frequency bands. The oscillator rejects commutative shifts
/// targeting an unleased or over-quota band.
pub struct Spectrum {
    pub size: usize,
    pub leases: HashMap<usize, BandLease>, // keyed by start_bin
}

impl Spectrum {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            leases: HashMap::new(),
        }
    }

    /// Attempt to lease a contiguous band. Returns `Ok(())` on success or an
    /// error if the band overlaps an existing lease.
    pub fn lease(
        &mut self,
        owner: AccountId,
        start_bin: usize,
        end_bin: usize,
        staked_amount: u128,
        throughput_quota: u64,
    ) -> Result<(), String> {
        if end_bin <= start_bin {
            return Err("end_bin must be greater than start_bin".to_string());
        }
        if end_bin > self.size {
            return Err("band exceeds spectrum size".to_string());
        }
        for lease in self.leases.values() {
            if start_bin < lease.end_bin && end_bin > lease.start_bin {
                return Err("band overlaps existing lease".to_string());
            }
        }
        self.leases.insert(
            start_bin,
            BandLease {
                owner,
                start_bin,
                end_bin,
                staked_amount,
                throughput_quota,
                used_throughput: 0,
            },
        );
        Ok(())
    }

    /// Find the lease covering `bin`, if any.
    pub fn lease_for(&self, bin: usize) -> Option<&BandLease> {
        self.leases
            .values()
            .find(|l| l.start_bin <= bin && bin < l.end_bin)
    }

    /// Find the mutable lease covering `bin`, if any.
    pub fn lease_for_mut(&mut self, bin: usize) -> Option<&mut BandLease> {
        self.leases
            .values_mut()
            .find(|l| l.start_bin <= bin && bin < l.end_bin)
    }

    /// Authorize a single commutative operation on `bin`. Increments quota usage.
    pub fn authorize(&mut self, bin: usize) -> Result<(), String> {
        let Some(lease) = self.lease_for_mut(bin) else {
            return Err(format!("bin {} is not leased", bin));
        };
        if !lease.consume_quota() {
            return Err(format!(
                "band [{}, {}) has exceeded throughput quota",
                lease.start_bin, lease.end_bin
            ));
        }
        Ok(())
    }

    /// Reset per-epoch quota counters (e.g., at the start of a new synthesis window).
    pub fn reset_quotas(&mut self) {
        for lease in self.leases.values_mut() {
            lease.used_throughput = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::KeyPair;

    #[test]
    fn spectrum_lease_and_authorize() {
        let mut spectrum = Spectrum::new(1024);
        let owner = KeyPair::generate().account_id();
        spectrum
            .lease(owner, 0, 64, 1_000_000_000_000, 100)
            .unwrap();
        assert!(spectrum.authorize(32).is_ok());
        assert!(spectrum.authorize(2000).is_err());
    }

    #[test]
    fn spectrum_rejects_overlapping_leases() {
        let mut spectrum = Spectrum::new(1024);
        let a = KeyPair::generate().account_id();
        let b = KeyPair::generate().account_id();
        spectrum.lease(a, 0, 64, 1_000_000_000_000, 100).unwrap();
        assert!(spectrum.lease(b, 32, 96, 1_000_000_000_000, 100).is_err());
    }
}
