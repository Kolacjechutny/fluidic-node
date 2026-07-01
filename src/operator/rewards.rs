use crate::crypto::{AccountId, DEFAULT_DEX_DOMAIN, PoolId};
use crate::operator::StakeTable;
use dashmap::DashMap;

/// Fraction of metabolic burn redirected to staked operators each tick, in
/// basis points (100 = 1%).
pub const OPERATOR_REWARD_BASIS_POINTS: u128 = 5000; // 50%

/// Fraction of metabolic burn redirected to liquidity providers each tick, in
/// basis points (100 = 1%).
pub const LP_REWARD_BASIS_POINTS: u128 = 5000; // 50%

/// Backwards-compatible alias for the operator reward share.
pub const REWARD_BASIS_POINTS: u128 = OPERATOR_REWARD_BASIS_POINTS;

/// Tracks accrued operator rewards from metabolic burn and fees.
///
/// Each metabolic burn is split 50/50: operators are paid proportional to their
/// stake, while the liquidity-provider share accrues to a per-pool balance that
/// LPs can claim.  For now all LP rewards accrue to the single DEX pool.
#[derive(Debug, Default)]
pub struct RewardPool {
    rewards: DashMap<AccountId, u128>,
    lp_rewards: DashMap<PoolId, u128>,
}

impl RewardPool {
    pub fn new() -> Self {
        Self {
            rewards: DashMap::new(),
            lp_rewards: DashMap::new(),
        }
    }

    /// Distribute a burned amount: the operator share is split across staked
    /// operators proportional to their stake, and the liquidity-provider share
    /// accrues to the DEX pool.  Any rounding remainder stays unallocated.
    pub fn distribute(&self, burned: u128, stake_table: &StakeTable) {
        if burned == 0 {
            return;
        }

        // Liquidity-provider share accrues to the DEX pool regardless of the
        // staked operator set.
        let lp_amount = burned.saturating_mul(LP_REWARD_BASIS_POINTS) / 10_000;
        if lp_amount > 0 {
            *self.lp_rewards.entry(DEFAULT_DEX_DOMAIN).or_insert(0) += lp_amount;
        }

        // Operator share is distributed proportional to stake.
        let reward_amount = burned.saturating_mul(OPERATOR_REWARD_BASIS_POINTS) / 10_000;
        if reward_amount == 0 {
            return;
        }

        let staked: Vec<(AccountId, u128)> = stake_table.staked_operators();

        let total_stake: u128 = staked.iter().map(|(_, s)| s).sum();
        if total_stake == 0 {
            return;
        }

        for (operator, stake) in staked {
            let share = reward_amount.saturating_mul(stake) / total_stake;
            if share > 0 {
                *self.rewards.entry(operator).or_insert(0) += share;
            }
        }
    }

    pub fn balance(&self, operator: &AccountId) -> u128 {
        self.rewards.get(operator).map(|e| *e.value()).unwrap_or(0)
    }

    pub fn claim(&self, operator: &AccountId) -> u128 {
        self.rewards
            .remove(operator)
            .map(|(_, amount)| amount)
            .unwrap_or(0)
    }

    /// Accrued liquidity-provider rewards for a pool.
    pub fn lp_reward_balance(&self, pool_id: PoolId) -> u128 {
        self.lp_rewards.get(&pool_id).map(|e| *e.value()).unwrap_or(0)
    }

    /// Claim (and zero out) the accrued liquidity-provider rewards for a pool.
    pub fn claim_lp_reward(&self, pool_id: PoolId) -> u128 {
        self.lp_rewards
            .remove(&pool_id)
            .map(|(_, amount)| amount)
            .unwrap_or(0)
    }

    /// Distribute fee revenue using the same 50/50 split as metabolic burn:
    /// operators proportional to stake, liquidity providers to the DEX pool.
    pub fn distribute_fees(&self, fees: u128, stake_table: &StakeTable) {
        self.distribute(fees, stake_table);
    }

    /// Cryptographic commitment over the current reward balances (operator and
    /// liquidity-provider shares).
    pub fn root(&self) -> [u8; 32] {
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = self
            .rewards
            .iter()
            .map(|e| (e.key().0.to_vec(), e.value().to_be_bytes().to_vec()))
            .collect();
        // Prefix LP-pool keys so they cannot collide with 32-byte account keys.
        for e in self.lp_rewards.iter() {
            let mut key = Vec::with_capacity(e.key().len() + 3);
            key.extend_from_slice(b"lp:");
            key.extend_from_slice(e.key());
            items.push((key, e.value().to_be_bytes().to_vec()));
        }
        items.sort_by(|(a, _), (b, _)| a.cmp(b));
        crate::state::MerkleAccumulator::root(&items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{AccountId, DEFAULT_DEX_DOMAIN};
    use crate::operator::{StakeTable, StakingConfig};

    #[test]
    fn rewards_split_proportionally_to_stake() {
        let stake = StakeTable::new(StakingConfig { min_stake: 1 });
        let a = AccountId([1u8; 32]);
        let b = AccountId([2u8; 32]);
        stake.stake(a, 75);
        stake.stake(b, 25);

        let pool = RewardPool::new();
        pool.distribute(10_000, &stake);

        // Operators share 50% (5_000) proportional to stake.
        assert_eq!(pool.balance(&a), 3_750); // 75% of 5_000
        assert_eq!(pool.balance(&b), 1_250); // 25% of 5_000
        // Liquidity providers receive the other 50%.
        assert_eq!(pool.lp_reward_balance(DEFAULT_DEX_DOMAIN), 5_000);
    }

    #[test]
    fn slashed_operator_gets_no_rewards() {
        let stake = StakeTable::new(StakingConfig { min_stake: 1 });
        let a = AccountId([1u8; 32]);
        let b = AccountId([2u8; 32]);
        stake.stake(a, 50);
        stake.stake(b, 50);
        stake.slash(a);

        let pool = RewardPool::new();
        pool.distribute(10_000, &stake);

        assert_eq!(pool.balance(&a), 0);
        assert_eq!(pool.balance(&b), 5_000);
        assert_eq!(pool.lp_reward_balance(DEFAULT_DEX_DOMAIN), 5_000);
    }

    #[test]
    fn lp_share_accrues_even_without_staked_operators() {
        let stake = StakeTable::new(StakingConfig { min_stake: 1_000 });
        // No operator meets the minimum stake.
        let pool = RewardPool::new();
        pool.distribute(10_000, &stake);

        // Operator share is unallocated, LP share still accrues.
        assert_eq!(pool.lp_reward_balance(DEFAULT_DEX_DOMAIN), 5_000);
    }

    #[test]
    fn claim_lp_reward_zeroes_balance() {
        let stake = StakeTable::new(StakingConfig { min_stake: 1 });
        let pool = RewardPool::new();
        pool.distribute(10_000, &stake);
        assert_eq!(pool.claim_lp_reward(DEFAULT_DEX_DOMAIN), 5_000);
        assert_eq!(pool.lp_reward_balance(DEFAULT_DEX_DOMAIN), 0);
    }
}
