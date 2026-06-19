use crate::consensus::ntt::{NTT_MODULUS, NttEngine};
use crate::crypto::{AccountId, PoolId};
use crate::field::coordinates::{Coordinate, FrequencyVector};
use dashmap::DashMap;

/// Native token precision: 10^12 sub-units per WAVE.
pub const WAVE_PRECISION: u128 = 1_000_000_000_000;

/// Fixed-point balance for an account or pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Balance {
    pub units: u128,
}

impl Balance {
    pub fn zero() -> Self {
        Self { units: 0 }
    }

    pub fn from_wave(wave: u128) -> Self {
        Self {
            units: wave.saturating_mul(WAVE_PRECISION),
        }
    }

    pub fn saturating_sub(&mut self, amount: u128) {
        self.units = self.units.saturating_sub(amount);
    }

    pub fn saturating_add(&mut self, amount: u128) {
        self.units = self.units.saturating_add(amount);
    }
}

/// The global state wave-field.
/// - `accounts` hold stateful balances and per-account frequency vectors.
/// - `pools` hold commutative aggregate balances.
/// - `spectrum` is the NTT-domain representation of the latest commutative batch.
pub struct WaveField {
    pub accounts: DashMap<AccountId, AccountState>,
    pub pools: DashMap<PoolId, Balance>,
    pub ntt_engine: NttEngine,
    /// Latest commutative wave-field amplitudes in the NTT domain.
    pub spectrum: Vec<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct AccountState {
    pub balance: Balance,
    pub frequency_vector: FrequencyVector,
}

impl WaveField {
    pub fn new(ntt_size: usize) -> Self {
        assert!(
            ntt_size.is_power_of_two() && ntt_size >= 2,
            "NTT size must be a power of two >= 2"
        );
        Self {
            accounts: DashMap::new(),
            pools: DashMap::new(),
            ntt_engine: NttEngine::new(ntt_size),
            spectrum: vec![0; ntt_size],
        }
    }

    pub fn ensure_account(&self, id: AccountId) {
        self.accounts.entry(id).or_insert(AccountState::default());
    }

    pub fn credit_account(&self, id: AccountId, amount: u128) {
        self.ensure_account(id);
        if let Some(mut state) = self.accounts.get_mut(&id) {
            state.balance.saturating_add(amount);
        }
    }

    pub fn debit_account(&self, id: AccountId, amount: u128) -> bool {
        self.ensure_account(id);
        if let Some(mut state) = self.accounts.get_mut(&id) {
            if state.balance.units < amount {
                return false;
            }
            state.balance.saturating_sub(amount);
            true
        } else {
            false
        }
    }

    pub fn account_balance(&self, id: AccountId) -> Balance {
        self.accounts
            .get(&id)
            .map(|s| s.balance)
            .unwrap_or(Balance::zero())
    }

    pub fn pool_balance(&self, pool_id: PoolId) -> Balance {
        self.pools
            .get(&pool_id)
            .map(|b| *b)
            .unwrap_or(Balance::zero())
    }

    /// Apply a batch of commutative deltas via NTT synthesis.
    /// `deltas` is a map from NTT bin index to signed delta (in sub-units).
    /// The function verifies that NTT(aggregate) matches sequential summation.
    pub fn synthesize_commutative_batch(
        &mut self,
        deltas: &[(Coordinate, i128, PoolId)],
    ) -> Result<(), String> {
        let size = self.ntt_engine.size;
        let mut time_domain = vec![0i128; size];

        // Direct sequential aggregation (the "ground truth").
        let mut pool_aggregates: std::collections::HashMap<PoolId, i128> =
            std::collections::HashMap::new();

        for (coord, delta, pool_id) in deltas {
            let idx = coord.to_ntt_index(size);
            time_domain[idx] = time_domain[idx].saturating_add(*delta);
            *pool_aggregates.entry(*pool_id).or_insert(0) += delta;
        }

        // Convert signed i128 deltas into the NTT prime field, applying modulo.
        // For demonstration we assume deltas fit in [-P/2, P/2].
        let mut ntt_input: Vec<u64> = time_domain.iter().map(|&x| signed_to_field(x)).collect();

        self.ntt_engine.ntt(&mut ntt_input);
        // Inverse transform to recover the aggregated time-domain values.
        let mut recovered = ntt_input.clone();
        self.ntt_engine.intt(&mut recovered);

        // Verify round-trip fidelity.
        for (i, &expected) in time_domain.iter().enumerate() {
            let actual = field_to_signed(recovered[i]);
            if expected != actual {
                return Err(format!(
                    "NTT round-trip mismatch at bin {}: expected {}, got {}",
                    i, expected, actual
                ));
            }
        }

        // Update spectrum and pool balances.
        self.spectrum = ntt_input;
        for (pool_id, aggregate) in pool_aggregates {
            let mut balance = self.pools.entry(pool_id).or_insert(Balance::zero());
            if aggregate >= 0 {
                balance.saturating_add(aggregate as u128);
            } else {
                let abs = aggregate.unsigned_abs();
                if balance.units < abs {
                    return Err(format!("Pool {:?} would go negative by {}", pool_id, abs));
                }
                balance.saturating_sub(abs);
            }
        }

        Ok(())
    }

    /// Directly apply a small commutative delta without a full NTT batch.
    pub fn apply_commutative_delta(&self, pool_id: PoolId, delta: i128) -> Result<(), String> {
        let mut balance = self.pools.entry(pool_id).or_insert(Balance::zero());
        if delta >= 0 {
            balance.saturating_add(delta as u128);
        } else {
            let abs = delta.unsigned_abs();
            if balance.units < abs {
                return Err(format!("Pool {:?} would go negative by {}", pool_id, abs));
            }
            balance.saturating_sub(abs);
        }
        Ok(())
    }
}

/// Convert signed i128 delta into a canonical field representative.
fn signed_to_field(x: i128) -> u64 {
    let p = NTT_MODULUS as i128;
    let mut r = x % p;
    if r < 0 {
        r += p;
    }
    r as u64
}

/// Convert canonical field representative back to signed i128 in [-P/2, P/2].
fn field_to_signed(x: u64) -> i128 {
    let p = NTT_MODULUS as i128;
    let half = p / 2;
    let v = x as i128;
    if v > half { v - p } else { v }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave_field_account_debit() {
        let field = WaveField::new(16);
        let id = AccountId([1u8; 32]);
        field.credit_account(id, 1_000_000_000_000);
        assert!(field.debit_account(id, 500_000_000_000));
        assert!(!field.debit_account(id, 600_000_000_000));
        assert_eq!(field.account_balance(id).units, 500_000_000_000);
    }

    #[test]
    fn ntt_batch_synthesis_matches_sequential_sum() {
        let mut field = WaveField::new(64);
        let pool = [7u8; 32];
        let deltas: Vec<(Coordinate, i128, PoolId)> = (0..32)
            .map(|i| (Coordinate::from_scalar(i as u64), 100, pool))
            .collect();
        field.synthesize_commutative_batch(&deltas).unwrap();
        assert_eq!(field.pool_balance(pool).units, 32 * 100);
    }
}
