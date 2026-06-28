use crate::crypto::AccountId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Staking configuration: the minimum amount an operator must lock to be
/// eligible to sign synthesis certificates.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

/// Operator entry in the stake table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorEntry {
    pub stake: u128,
    /// Slash nonce. `None` means not slashed.
    pub slash_nonce: Option<u64>,
}

/// In-memory stake table for permissioned operators. The table can be
/// snapshotted to disk so stake state survives node restarts.
#[derive(Debug)]
pub struct StakeTable {
    config: StakingConfig,
    operators: RwLock<HashMap<AccountId, OperatorEntry>>,
    slash_counter: AtomicU64,
}

impl Default for StakeTable {
    fn default() -> Self {
        Self::new(StakingConfig::default())
    }
}

impl StakeTable {
    pub fn new(config: StakingConfig) -> Self {
        Self {
            config,
            operators: RwLock::new(HashMap::new()),
            slash_counter: AtomicU64::new(0),
        }
    }

    pub fn min_stake(&self) -> u128 {
        self.config.min_stake
    }

    pub fn config(&self) -> &StakingConfig {
        &self.config
    }

    /// Lock stake for an operator. Replaces any existing stake.
    pub fn stake(&self, operator: AccountId, amount: u128) {
        let mut ops = self.operators.write().unwrap();
        let entry = ops.entry(operator).or_insert_with(|| OperatorEntry {
            stake: 0,
            slash_nonce: None,
        });
        entry.stake = amount;
    }

    pub fn get_stake(&self, operator: &AccountId) -> u128 {
        self.operators
            .read()
            .unwrap()
            .get(operator)
            .map(|e| e.stake)
            .unwrap_or(0)
    }

    pub fn staked_operators(&self) -> Vec<(AccountId, u128)> {
        self.operators
            .read()
            .unwrap()
            .iter()
            .filter(|(k, e)| self.is_staked_inner(k, e))
            .map(|(k, e)| (*k, e.stake))
            .collect()
    }

    pub fn total_stake(&self) -> u128 {
        self.operators
            .read()
            .unwrap()
            .values()
            .map(|e| e.stake)
            .sum()
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

    fn is_staked_inner(&self, _operator: &AccountId, entry: &OperatorEntry) -> bool {
        entry.stake >= self.config.min_stake && entry.slash_nonce.is_none()
    }

    pub fn is_staked(&self, operator: &AccountId) -> bool {
        let ops = self.operators.read().unwrap();
        ops.get(operator)
            .map(|e| self.is_staked_inner(operator, e))
            .unwrap_or(false)
    }

    pub fn slash(&self, operator: AccountId) -> u64 {
        let nonce = self.slash_counter.fetch_add(1, Ordering::SeqCst);
        let mut ops = self.operators.write().unwrap();
        let entry = ops.entry(operator).or_insert_with(|| OperatorEntry {
            stake: 0,
            slash_nonce: None,
        });
        entry.slash_nonce = Some(nonce);
        nonce
    }

    pub fn is_slashed(&self, operator: &AccountId) -> bool {
        self.operators
            .read()
            .unwrap()
            .get(operator)
            .map(|e| e.slash_nonce.is_some())
            .unwrap_or(false)
    }

    /// Cryptographic commitment over the current stake table.
    pub fn root(&self) -> [u8; 32] {
        let ops = self.operators.read().unwrap();
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = ops
            .iter()
            .map(|(k, e)| {
                let mut value = Vec::with_capacity(40);
                value.extend_from_slice(&e.stake.to_be_bytes());
                value.push(if e.slash_nonce.is_some() { 1 } else { 0 });
                (k.0.to_vec(), value)
            })
            .collect();
        items.sort_by(|(a, _), (b, _)| a.cmp(b));
        crate::state::MerkleAccumulator::root(&items)
    }

    /// Serialize the table for persistence.
    pub fn to_snapshot(&self) -> BTreeMap<String, OperatorEntry> {
        let ops = self.operators.read().unwrap();
        let mut map = BTreeMap::new();
        for (k, v) in ops.iter() {
            map.insert(hex::encode(k.0), v.clone());
        }
        map
    }

    /// Restore the table from a snapshot.
    pub fn from_snapshot(config: StakingConfig, snapshot: BTreeMap<String, OperatorEntry>) -> Self {
        let mut operators = HashMap::with_capacity(snapshot.len());
        let mut max_slash_nonce = 0u64;
        for (hex, entry) in snapshot {
            let bytes = match hex::decode(&hex) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if bytes.len() != 32 {
                continue;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            if let Some(nonce) = entry.slash_nonce {
                max_slash_nonce = max_slash_nonce.max(nonce);
            }
            operators.insert(AccountId(arr), entry);
        }
        Self {
            config,
            operators: RwLock::new(operators),
            slash_counter: AtomicU64::new(max_slash_nonce + 1),
        }
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

    #[test]
    fn snapshot_roundtrip_preserves_stake_and_slash() {
        let table = StakeTable::new(StakingConfig { min_stake: 100 });
        let op = AccountId([3u8; 32]);
        table.stake(op, 200);
        table.slash(op);
        let snap = table.to_snapshot();
        let restored = StakeTable::from_snapshot(StakingConfig { min_stake: 100 }, snap);
        assert_eq!(restored.get_stake(&op), 200);
        assert!(restored.is_slashed(&op));
        assert!(!restored.is_staked(&op));
    }
}
