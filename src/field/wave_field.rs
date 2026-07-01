use crate::consensus::ntt::{NTT_MODULUS, NttEngine};
use crate::crypto::{AccountId, DEFAULT_DEX_DOMAIN, DomainId, PoolId};
use crate::field::coordinates::{Coordinate, FrequencyVector};
use crate::value::metabolic::{DEFAULT_DEX_LAMBDA_PPM, decayed_balance};
use dashmap::DashMap;
use std::collections::HashSet;

/// Native token precision: 10^12 sub-units per WAVE.
pub const WAVE_PRECISION: u128 = 1_000_000_000_000;

/// Fixed-point balance for an account or pool.
///
/// Each balance carries the metabolic-decay bookkeeping needed to apply
/// `B(t) = B(0) * e^(-λt)` lazily: `last_decay_tick` records the synthesis tick
/// the value was last decayed to, and `domain`/`rate_ppm` identify which
/// domain's decay constant governs it.  For now every balance is tracked under
/// the DEX domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Balance {
    pub units: u128,
    /// Synthesis tick this balance was last decayed to.
    pub last_decay_tick: u64,
    /// Concurrency domain governing this balance's decay constant.
    pub domain: DomainId,
    /// Per-domain decay constant λ in parts-per-million per tick.
    pub rate_ppm: u64,
    /// Whether this balance is subject to metabolic decay.  Metabolic decay is
    /// WAVE's native monetary policy, so WAVE balances decay (`true`) while
    /// foreign value such as USDC and bridged assets are exempt (`false`).
    pub decays: bool,
    /// Synthesis tick at which this account last transacted.  Accounts that
    /// transacted within the activity grace window are exempt from decay.  A
    /// value of `0` means "never transacted" and does not grant grace.
    pub last_active_tick: u64,
}

impl Default for Balance {
    fn default() -> Self {
        Self {
            units: 0,
            last_decay_tick: 0,
            domain: DEFAULT_DEX_DOMAIN,
            rate_ppm: DEFAULT_DEX_LAMBDA_PPM,
            decays: true,
            last_active_tick: 0,
        }
    }
}

impl Balance {
    pub fn zero() -> Self {
        Self::default()
    }

    pub fn from_wave(wave: u128) -> Self {
        Self {
            units: wave.saturating_mul(WAVE_PRECISION),
            ..Self::default()
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

    /// Mark an account's balance as exempt from metabolic decay (e.g. a USDC or
    /// bridged-asset token account).  Idempotent; creates the account if absent.
    pub fn set_non_decaying(&self, id: AccountId) {
        self.ensure_account(id);
        if let Some(mut state) = self.accounts.get_mut(&id) {
            state.balance.decays = false;
        }
    }

    /// Record that an account transacted at synthesis `tick`, starting its
    /// activity grace window.  Idempotent; creates the account if absent.
    pub fn mark_active(&self, id: AccountId, tick: u64) {
        self.ensure_account(id);
        if let Some(mut state) = self.accounts.get_mut(&id) {
            state.balance.last_active_tick = tick;
        }
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

    /// Apply exponential metabolic decay to every account and pool balance,
    /// advancing each to synthesis `tick`.
    ///
    /// Decay follows the closed-form curve `B(t) = B(0) * (1_000_000 - λ)^Δ /
    /// 1_000_000^Δ`, where `Δ = tick - last_decay_tick` for each balance.  Staked
    /// operators listed in `immune_accounts` are exempt: their balances are not
    /// decayed, but their decay clock is still advanced so a later loss of
    /// immunity does not trigger a large catch-up burn.
    ///
    /// Returns the total value burned across all balances this call.
    pub fn apply_metabolic_decay(
        &mut self,
        tick: u64,
        rate_ppm: u64,
        immune_accounts: &HashSet<AccountId>,
    ) -> u128 {
        let mut total_burned = 0u128;

        for mut entry in self.accounts.iter_mut() {
            if immune_accounts.contains(entry.key()) {
                entry.value_mut().balance.last_decay_tick = tick;
                continue;
            }
            let burned = decay_balance_in_place(&mut entry.value_mut().balance, tick, rate_ppm);
            total_burned = total_burned.saturating_add(burned);
        }

        for mut entry in self.pools.iter_mut() {
            let burned = decay_balance_in_place(entry.value_mut(), tick, rate_ppm);
            total_burned = total_burned.saturating_add(burned);
        }

        total_burned
    }
}

/// Decay a single balance in place to synthesis `tick`, returning the burned
/// amount.  The balance's `last_decay_tick` is always advanced to `tick`.
fn decay_balance_in_place(balance: &mut Balance, tick: u64, rate_ppm: u64) -> u128 {
    // Foreign value (USDC, bridged assets) does not decay; only WAVE does.
    if !balance.decays {
        balance.last_decay_tick = tick;
        return 0;
    }
    // Activity grace: an account that transacted within the grace window is
    // exempt from decay.  `last_active_tick == 0` means "never transacted" and
    // does not grant grace, so seeded-but-idle balances still decay.
    if balance.last_active_tick != 0
        && tick.saturating_sub(balance.last_active_tick)
            <= crate::value::metabolic::METABOLIC_IDLE_GRACE_TICKS
    {
        balance.last_decay_tick = tick;
        return 0;
    }
    let elapsed = tick.saturating_sub(balance.last_decay_tick);
    if elapsed == 0 || balance.units == 0 {
        balance.last_decay_tick = tick;
        return 0;
    }
    let remaining = decayed_balance(balance.units, rate_ppm, elapsed);
    let burned = balance.units.saturating_sub(remaining);
    balance.units = remaining;
    balance.last_decay_tick = tick;
    burned
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
    use crate::value::metabolic::METABOLIC_IDLE_GRACE_TICKS;

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

    #[test]
    fn metabolic_decay_follows_exponential_formula() {
        let mut field = WaveField::new(16);
        let id = AccountId([5u8; 32]);
        let initial = 1_000_000_000_000u128; // 1 WAVE
        field.credit_account(id, initial);

        let rate_ppm = 10_000; // 1% per tick (10_000 / 1_000_000)
        let n = 5u64;
        let immune = HashSet::new();
        let burned = field.apply_metabolic_decay(n, rate_ppm, &immune);

        // The remaining balance must equal the closed-form exponential curve.
        let expected_remaining = decayed_balance(initial, rate_ppm, n);
        assert_eq!(field.account_balance(id).units, expected_remaining);
        assert_eq!(burned, initial - expected_remaining);
        assert_eq!(field.account_balance(id).last_decay_tick, n);

        // A second decay continues the curve from where it left off.
        let burned2 = field.apply_metabolic_decay(n + 1, rate_ppm, &immune);
        let expected2 = decayed_balance(expected_remaining, rate_ppm, 1);
        assert_eq!(field.account_balance(id).units, expected2);
        assert_eq!(burned2, expected_remaining - expected2);
    }

    #[test]
    fn metabolic_decay_skips_immune_accounts() {
        let mut field = WaveField::new(16);
        let immune_id = AccountId([6u8; 32]);
        let normal_id = AccountId([7u8; 32]);
        field.credit_account(immune_id, 1_000_000);
        field.credit_account(normal_id, 1_000_000);

        let mut immune = HashSet::new();
        immune.insert(immune_id);
        let burned = field.apply_metabolic_decay(10, 100, &immune);

        // Immune balance untouched, but its decay clock still advances.
        assert_eq!(field.account_balance(immune_id).units, 1_000_000);
        assert_eq!(field.account_balance(immune_id).last_decay_tick, 10);
        // Normal balance decays and accounts for all of the burn.
        let expected = decayed_balance(1_000_000, 100, 10);
        assert_eq!(field.account_balance(normal_id).units, expected);
        assert_eq!(burned, 1_000_000 - expected);
    }

    #[test]
    fn metabolic_decay_also_decays_pool_balances() {
        let mut field = WaveField::new(16);
        let pool = [8u8; 32];
        field.apply_commutative_delta(pool, 1_000_000).unwrap();

        let immune = HashSet::new();
        let burned = field.apply_metabolic_decay(3, 100, &immune);
        let expected = decayed_balance(1_000_000, 100, 3);
        assert_eq!(field.pool_balance(pool).units, expected);
        assert_eq!(burned, 1_000_000 - expected);
    }

    #[test]
    fn metabolic_decay_exempts_non_decaying_balances() {
        let mut field = WaveField::new(16);
        let wave_id = AccountId([10u8; 32]);
        let usdc_id = AccountId([11u8; 32]);
        field.credit_account(wave_id, 1_000_000);
        field.credit_account(usdc_id, 1_000_000);
        // USDC is foreign value: exempt from decay.
        field.set_non_decaying(usdc_id);

        let immune = HashSet::new();
        let burned = field.apply_metabolic_decay(10, 100, &immune);

        // The non-decaying (USDC) balance is untouched; its clock still advances.
        assert_eq!(field.account_balance(usdc_id).units, 1_000_000);
        assert_eq!(field.account_balance(usdc_id).last_decay_tick, 10);
        // Only the WAVE balance decays and accounts for all of the burn.
        let expected = decayed_balance(1_000_000, 100, 10);
        assert_eq!(field.account_balance(wave_id).units, expected);
        assert_eq!(burned, 1_000_000 - expected);
    }

    #[test]
    fn metabolic_decay_grants_activity_grace() {
        let mut field = WaveField::new(16);
        let id = AccountId([12u8; 32]);
        let initial = 1_000_000u128;
        field.credit_account(id, initial);
        // The account transacted at tick 5, starting its grace window.
        field.mark_active(id, 5);

        let immune = HashSet::new();
        // Within the grace window: balance is untouched and nothing burns, but
        // the decay clock still advances.
        let burned = field.apply_metabolic_decay(7, 10_000, &immune);
        assert_eq!(field.account_balance(id).units, initial);
        assert_eq!(burned, 0);
        assert_eq!(field.account_balance(id).last_decay_tick, 7);

        // Beyond the grace window the balance decays from where the clock left
        // off (tick 7 -> tick 4*60*60 + 7 + 1, i.e. 4h + 1 ticks later).
        let burned2 = field.apply_metabolic_decay(5 + METABOLIC_IDLE_GRACE_TICKS + 1, 10_000, &immune);
        let expected = decayed_balance(initial, 10_000, (5 + METABOLIC_IDLE_GRACE_TICKS + 1) - 7);
        assert_eq!(field.account_balance(id).units, expected);
        assert!(expected < initial);
        assert_eq!(burned2, initial - expected);
    }
}
