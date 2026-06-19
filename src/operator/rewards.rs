use crate::crypto::AccountId;
use crate::operator::StakeTable;
use dashmap::DashMap;

/// Fraction of metabolic burn that is redirected to staked operators each tick.
/// Expressed in basis points (100 = 1%).
pub const REWARD_BASIS_POINTS: u128 = 5000; // 50%

/// Tracks accrued operator rewards from metabolic burn and fees.
#[derive(Debug, Default)]
pub struct RewardPool {
    rewards: DashMap<AccountId, u128>,
}

impl RewardPool {
    pub fn new() -> Self {
        Self {
            rewards: DashMap::new(),
        }
    }

    /// Distribute a portion of the burned amount across all staked operators
    /// proportional to their stake.  Any rounding remainder stays in the pool
    /// as unallocated burn.
    pub fn distribute(&self, burned: u128, stake_table: &StakeTable) {
        if burned == 0 {
            return;
        }
        let reward_amount = burned.saturating_mul(REWARD_BASIS_POINTS) / 10_000;
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

    /// Cryptographic commitment over the current reward balances.
    pub fn root(&self) -> [u8; 32] {
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = self
            .rewards
            .iter()
            .map(|e| (e.key().0.to_vec(), e.value().to_be_bytes().to_vec()))
            .collect();
        items.sort_by(|(a, _), (b, _)| a.cmp(b));
        crate::state::MerkleAccumulator::root(&items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::AccountId;
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

        assert_eq!(pool.balance(&a), 3_750); // 75% of 5_000
        assert_eq!(pool.balance(&b), 1_250); // 25% of 5_000
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
    }
}
