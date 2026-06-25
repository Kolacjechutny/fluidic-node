use crate::consensus::dag::{DagError, ShiftStatus};
use crate::consensus::Oscillator;
use crate::crypto::{AccountId, StatefulShift};
use crate::field::wave_field::{AccountState, Balance};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};


/// On-disk snapshot of the entire oscillator state.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    accounts: Vec<(String, AccountStateSer)>,
    pools: Vec<(String, u128)>,
    dag_nodes: Vec<DagNodeSer>,
    dag_balances: Vec<(String, u128)>,
    total_burned: u128,
}

#[derive(Serialize, Deserialize)]
struct AccountStateSer {
    units: u128,
}

#[derive(Serialize, Deserialize)]
struct DagNodeSer {
    hash: String,
    shift: StatefulShift,
    children: Vec<String>,
    inserted_at_tick: u64,
    #[serde(default = "default_finalization_depth")]
    finalization_depth: u64,
    #[serde(default)]
    first_seen_at_ns: u64,
    status: String,
    error: Option<String>,
}

fn default_finalization_depth() -> u64 {
    crate::consensus::dag::VectorClockDag::FINALIZATION_DEPTH
}

fn account_to_hex(a: &AccountId) -> String {
    hex::encode(a.0)
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

fn pool_to_hex(p: &crate::crypto::PoolId) -> String {
    hex::encode(p)
}

fn pool_from_hex(s: &str) -> Option<crate::crypto::PoolId> {
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(arr)
}

fn hash_to_hex(h: &[u8; 32]) -> String {
    hex::encode(h)
}

fn hash_from_hex(s: &str) -> Option<[u8; 32]> {
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(arr)
}

/// Persist oscillator state to `path`.
pub fn save(osc: &Oscillator, path: impl AsRef<Path>) -> Result<(), String> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    // Consistent lock order with the rest of the oscillator: dag first, then wave_field.
    let dag = osc.dag.lock().map_err(|e| e.to_string())?;
    let wave = osc.wave_field.lock().map_err(|e| e.to_string())?;

    let accounts: Vec<_> = wave
        .accounts
        .iter()
        .map(|entry| {
            (
                account_to_hex(entry.key()),
                AccountStateSer {
                    units: entry.value().balance.units,
                },
            )
        })
        .collect();

    let pools: Vec<_> = wave
        .pools
        .iter()
        .map(|entry| (pool_to_hex(entry.key()), entry.value().units))
        .collect();

    let dag_nodes: Vec<_> = dag
        .nodes
        .values()
        .map(|node| DagNodeSer {
            hash: hash_to_hex(&node.hash),
            shift: node.shift.clone(),
            children: node.children.iter().map(hash_to_hex).collect(),
            inserted_at_tick: node.inserted_at_tick,
            finalization_depth: node.finalization_depth,
            first_seen_at_ns: node.first_seen_at_ns,
            status: match node.status {
                ShiftStatus::Accepted => "accepted".to_string(),
                ShiftStatus::Finalized => "finalized".to_string(),
                ShiftStatus::Rejected(ref err) => format!("rejected:{}", dag_error_code(err)),
            },
            error: match node.status {
                ShiftStatus::Rejected(ref err) => Some(dag_error_code(err)),
                _ => None,
            },
        })
        .collect();

    let dag_balances: Vec<_> = dag
        .balances
        .iter()
        .map(|(k, v)| (account_to_hex(k), *v))
        .collect();

    let total_burned = *osc
        .metabolic_engine
        .total_burned
        .lock()
        .map_err(|e| e.to_string())?;

    let snapshot = Snapshot {
        version: 1,
        accounts,
        pools,
        dag_nodes,
        dag_balances,
        total_burned,
    };

    let tmp = path.with_extension("tmp");
    let json = serde_json::to_string_pretty(&snapshot).map_err(|e| e.to_string())?;
    fs::write(&tmp, json).map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).map_err(|e| e.to_string())?;

    drop(wave);
    drop(dag);
    Ok(())
}

/// Load oscillator state from `path` into an existing oscillator.
pub fn load(osc: &Oscillator, path: impl AsRef<Path>) -> Result<(), String> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(());
    }

    let json = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let snapshot: Snapshot = serde_json::from_str(&json).map_err(|e| e.to_string())?;

    if snapshot.version != 1 {
        return Err(format!("unsupported snapshot version {}", snapshot.version));
    }

    // Consistent lock order with synthesis: dag first, then wave_field.
    let mut dag = osc.dag.lock().map_err(|e| e.to_string())?;
    let wave = osc.wave_field.lock().map_err(|e| e.to_string())?;

    wave.accounts.clear();
    for (hex, state) in snapshot.accounts {
        if let Some(id) = account_from_hex(&hex) {
            wave.accounts.insert(
                id,
                AccountState {
                    balance: Balance { units: state.units },
                    ..Default::default()
                },
            );
        }
    }

    wave.pools.clear();
    for (hex, units) in snapshot.pools {
        if let Some(id) = pool_from_hex(&hex) {
            wave.pools.insert(id, Balance { units });
        }
    }

    dag.nodes.clear();
    dag.roots.clear();
    dag.tips.clear();
    dag.balances.clear();
    dag.rejected.clear();

    // First pass: insert nodes.
    for node in &snapshot.dag_nodes {
        if let Some(hash) = hash_from_hex(&node.hash) {
            dag.nodes.insert(
                hash,
                crate::consensus::dag::DagNode {
                    hash,
                    shift: node.shift.clone(),
                    children: node.children.iter().filter_map(|h| hash_from_hex(h)).collect(),
                    inserted_at_tick: node.inserted_at_tick,
                    finalization_depth: node.finalization_depth,
                    first_seen_at_ns: node.first_seen_at_ns,
                    status: parse_status(&node.status, &node.error),
                },
            );
        }
    }

    // Rebuild roots.
    let roots: Vec<_> = dag
        .nodes
        .iter()
        .filter(|(_, node)| node.shift.predecessors.is_empty())
        .map(|(hash, _)| *hash)
        .collect();
    for hash in roots {
        dag.roots.insert(hash);
    }

    // Rebuild tips.
    for node in snapshot.dag_nodes {
        if let Some(hash) = hash_from_hex(&node.hash) {
            let from = dag.nodes.get(&hash).map(|n| n.shift.from);
            if let Some(from) = from {
                dag.tips.insert(from, hash);
            }
        }
    }

    for (hex, units) in snapshot.dag_balances {
        if let Some(id) = account_from_hex(&hex) {
            dag.balances.insert(id, units);
        }
    }

    drop(wave);
    drop(dag);

    if let Ok(mut burned) = osc.metabolic_engine.total_burned.lock() {
        *burned = snapshot.total_burned;
    }

    Ok(())
}

fn parse_status(status: &str, error: &Option<String>) -> ShiftStatus {
    if let Some(code) = error.as_deref() {
        return ShiftStatus::Rejected(parse_dag_error(code));
    }
    if let Some(code) = status.strip_prefix("rejected:") {
        return ShiftStatus::Rejected(parse_dag_error(code));
    }
    match status {
        "finalized" => ShiftStatus::Finalized,
        _ => ShiftStatus::Accepted,
    }
}

fn dag_error_code(err: &DagError) -> String {
    match err {
        DagError::MissingPredecessor(_) => "missing_predecessor",
        DagError::InvalidSignature(_) => "invalid_signature",
        DagError::InsufficientBalance(_) => "insufficient_balance",
        DagError::DoubleSpend(_) => "double_spend",
        DagError::CausalCycle(_) => "causal_cycle",
    }
    .to_string()
}

fn parse_dag_error(code: &str) -> DagError {
    match code {
        "invalid_signature" => DagError::InvalidSignature([0u8; 32]),
        "insufficient_balance" => DagError::InsufficientBalance([0u8; 32]),
        "double_spend" => DagError::DoubleSpend([0u8; 32]),
        "causal_cycle" => DagError::CausalCycle([0u8; 32]),
        _ => DagError::MissingPredecessor([0u8; 32]),
    }
}

pub fn snapshot_path() -> PathBuf {
    let dir = std::env::var("FLUIDIC_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    PathBuf::from(dir).join("snapshot.json")
}
