use crate::crypto::AccountId;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Staking configuration: the minimum amount an operator must lock to be
/// eligible to sign synthesis certificates.
#[derive(Clone, Debug)]
pub struct StakingConfig {
    pub min_stake: u128,
}

impl Default for StakingConfig {
    fn default() -> Self {
        Self {
            // 1 million WAVE (10^12 sub-units per WAVE).
            min_stake: 1_000_000_000_000_000_000,
        }
    }
}

/// In-memory stake table for permissioned operators.
#[derive(Debug, Default)]
pub struct StakeTable {
    config: StakingConfig,
    stakes: DashMap<AccountId, u128>,
    slashes: DashMap<AccountId, u64>,
    slash_nonce: AtomicU64,
}

impl StakeTable {
    pub fn new(config: StakingConfig) -> Self {
        Self {
            config,
            stakes: DashMap::new(),
            slashes: DashMap::new(),
            slash_nonce: AtomicU64::new(0),
        }
    }

    pub fn min_stake(&self) -> u128 {
        self.config.min_stake
    }

    /// Lock stake for an operator.  Replaces any existing stake.
    pub fn stake(&self, operator: AccountId, amount: u128) {
        self.stakes.insert(operator, amount);
    }

    pub fn get_stake(&self, operator: &AccountId) -> u128 {
        self.stakes.get(operator).map(|e| *e.value()).unwrap_or(0)
    }

    pub fn staked_operators(&self) -> Vec<(AccountId, u128)> {
        self.stakes
            .iter()
            .filter(|e| self.is_staked(e.key()))
            .map(|e| (*e.key(), *e.value()))
            .collect()
    }

    pub fn total_stake(&self) -> u128 {
        self.stakes.iter().map(|e| *e.value()).sum()
    }

    /// Stake required to form a Byzantine-fault-tolerant quorum (>2/3).
    pub fn quorum_threshold(&self) -> u128 {
        let total = self.total_stake();
        if total == 0 {
            return 0;
        }
        // Strictly more than 2/3.
        total / 3 * 2 + 1
    }

    pub fn is_staked(&self, operator: &AccountId) -> bool {
        self.get_stake(operator) >= self.config.min_stake && !self.is_slashed(operator)
    }

    pub fn slash(&self, operator: AccountId) -> u64 {
        let nonce = self.slash_nonce.fetch_add(1, Ordering::SeqCst);
        self.slashes.insert(operator, nonce);
        nonce
    }

    pub fn is_slashed(&self, operator: &AccountId) -> bool {
        self.slashes.contains_key(operator)
    }

    /// Cryptographic commitment over the current stake table.
    pub fn root(&self) -> [u8; 32] {
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = self
            .stakes
            .iter()
            .map(|e| {
                let mut value = Vec::with_capacity(40);
                value.extend_from_slice(&e.value().to_be_bytes());
                value.extend_from_slice(if self.is_slashed(e.key()) { b"1" } else { b"0" });
                (e.key().0.to_vec(), value)
            })
            .collect();
        items.sort_by(|(a, _), (b, _)| a.cmp(b));
        crate::state::MerkleAccumulator::root(&items)
    }

    /// Load comma-separated `account=amount` stakes from a string.
    pub fn from_spec(config: StakingConfig, spec: &str) -> Self {
        let table = Self::new(config);
        for entry in spec.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((hex, amount_str)) = entry.split_once('=') else {
                continue;
            };
            let Some(account) = account_from_hex(hex.trim()) else {
                continue;
            };
            let Ok(amount) = amount_str.trim().parse::<u128>() else {
                continue;
            };
            table.stake(account, amount);
        }
        table
    }
}

fn account_from_hex(s: &str) -> Option<AccountId> {
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(AccountId(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staked_operator_meets_minimum() {
        let table = StakeTable::new(StakingConfig { min_stake: 100 });
        let op = AccountId([1u8; 32]);
        assert!(!table.is_staked(&op));
        table.stake(op, 100);
        assert!(table.is_staked(&op));
    }

    #[test]
    fn slashed_operator_is_no_longer_staked() {
        let table = StakeTable::new(StakingConfig { min_stake: 100 });
        let op = AccountId([2u8; 32]);
        table.stake(op, 100);
        table.slash(op);
        assert!(!table.is_staked(&op));
    }
}
