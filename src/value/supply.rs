use crate::field::wave_field::WAVE_PRECISION;
use std::sync::Mutex;

/// Fixed maximum supply of WAVE: 1 billion tokens at 10^12 sub-units each.
pub const TOTAL_WAVE_SUPPLY: u128 = 1_000_000_000u128 * WAVE_PRECISION;

/// Tracks circulating and burned supply for supply-cap enforcement.
///
/// All minting (genesis, registration/faucet, EVM faucet) must flow through
/// [`mint`](Self::mint).  Burning is fed by slashing.
#[derive(Debug, Default)]
pub struct SupplyTracker {
    /// WAVE that has entered circulation through minting.
    circulating: Mutex<u128>,
    /// WAVE that has left circulation through burning.
    burned: Mutex<u128>,
}

impl SupplyTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt to mint `amount` sub-units.  Returns `true` on success and
    /// increments `circulating`.  Returns `false` if the cap would be exceeded.
    pub fn mint(&self, amount: u128) -> bool {
        if amount == 0 {
            return true;
        }
        let mut circ = self.circulating.lock().unwrap();
        if circ.saturating_add(amount) > TOTAL_WAVE_SUPPLY {
            return false;
        }
        *circ += amount;
        true
    }

    /// Record burned supply.  Burned tokens are subtracted from circulating and
    /// added to the burned counter, freeing cap space.
    pub fn burn(&self, amount: u128) {
        if amount == 0 {
            return;
        }
        let mut circ = self.circulating.lock().unwrap();
        let mut burned = self.burned.lock().unwrap();
        *circ = circ.saturating_sub(amount);
        *burned += amount;
    }

    pub fn circulating(&self) -> u128 {
        *self.circulating.lock().unwrap()
    }

    pub fn burned(&self) -> u128 {
        *self.burned.lock().unwrap()
    }

    pub fn remaining(&self) -> u128 {
        TOTAL_WAVE_SUPPLY.saturating_sub(self.circulating())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_increases_circulating() {
        let supply = SupplyTracker::new();
        assert!(supply.mint(1_000));
        assert_eq!(supply.circulating(), 1_000);
        assert_eq!(supply.remaining(), TOTAL_WAVE_SUPPLY - 1_000);
    }

    #[test]
    fn mint_respects_cap() {
        let supply = SupplyTracker::new();
        assert!(supply.mint(TOTAL_WAVE_SUPPLY));
        assert!(!supply.mint(1));
    }

    #[test]
    fn burn_frees_cap_space() {
        let supply = SupplyTracker::new();
        assert!(supply.mint(1_000));
        supply.burn(400);
        assert_eq!(supply.circulating(), 600);
        assert_eq!(supply.burned(), 400);
        assert!(supply.mint(400));
        assert_eq!(supply.circulating(), 1_000);
    }
}
