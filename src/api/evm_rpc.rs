use crate::api::state::{ApiState, RecentShift};
use crate::evm::{block_hash_for, EvmTransaction, FLUIDIC_EVM_CHAIN_ID};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use ethers_core::types::{Address as EvmAddress, H256, U256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Fluidic testnet chain ID.  Chosen as a fixed, unlikely-to-collide value.
pub const FLUIDIC_CHAIN_ID: u64 = FLUIDIC_EVM_CHAIN_ID;

#[derive(Clone, Debug, Deserialize)]
struct RpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Vec<Value>,
    id: Value,
}

#[derive(Clone, Debug, Serialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Clone, Debug, Serialize)]
struct RpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
    id: Value,
}

#[derive(Clone, Debug, Deserialize)]
struct MinTickQuery {
    #[serde(default)]
    min_tick: Option<u64>,
}

/// Wait until the local oscillator has synthesized at least `min_tick`, with a
/// timeout to avoid blocking forever on isolated nodes.
async fn wait_for_min_tick(state: &ApiState, min_tick: Option<u64>) {
    let Some(min_tick) = min_tick else {
        return;
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while state.oscillator.synthesis_tick.load(std::sync::atomic::Ordering::SeqCst) < min_tick {
        if std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub fn evm_rpc_router() -> Router<Arc<ApiState>> {
    Router::new().route("/rpc", post(rpc_handler))
}

async fn rpc_handler(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<MinTickQuery>,
    Json(req): Json<RpcRequest>,
) -> Response {
    let id = req.id.clone();
    match dispatch(state, query.min_tick, req).await {
        Ok(result) => Json(RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        })
        .into_response(),
        Err((code, message)) => {
            let status = if code == -32601 {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            };
            (
                status,
                Json(RpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(RpcError { code, message }),
                    id,
                }),
            )
                .into_response()
        }
    }
}

async fn dispatch(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    req: RpcRequest,
) -> Result<Value, (i64, String)> {
    match req.method.as_str() {
        "eth_blockNumber" => {
            wait_for_min_tick(&state, min_tick).await;
            Ok(block_number(state).await)
        }
        "eth_getBlockByNumber" => get_block_by_number(state, min_tick, req.params).await,
        "eth_getBlockByHash" => get_block_by_hash(state, min_tick, req.params).await,
        "eth_chainId" => Ok(chain_id().await),
        "net_version" => Ok(chain_id().await),
        "eth_gasPrice" => Ok(Value::String("0x0".to_string())),
        "eth_getBalance" => get_balance(state, min_tick, req.params).await,
        "eth_getCode" => get_code(state, min_tick, req.params).await,
        "eth_call" => eth_call(state, min_tick, req.params).await,
        "eth_sendRawTransaction" => send_raw_transaction(state, req.params).await,
        "eth_getTransactionByHash" => get_transaction_by_hash(state, min_tick, req.params).await,
        "eth_getTransactionReceipt" => get_transaction_receipt(state, min_tick, req.params).await,
        "eth_getTransactionCount" => get_transaction_count(state, min_tick, req.params).await,
        "eth_estimateGas" => estimate_gas(state, min_tick, req.params).await,
        "eth_getLogs" => get_logs(state, min_tick, req.params).await,
        "web3_clientVersion" => Ok(Value::String("fluidic/0.1.0".to_string())),
        _ => Err((
            -32601,
            format!("Method not found: {}", req.method),
        )),
    }
}

async fn block_number(state: Arc<ApiState>) -> Value {
    let tick = state
        .oscillator
        .synthesis_tick
        .load(std::sync::atomic::Ordering::SeqCst);
    Value::String(format!("0x{:x}", tick))
}

fn parse_hex_hash(s: &str) -> Result<H256, (i64, String)> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| (-32602, format!("invalid hex: {}", e)))?;
    if bytes.len() != 32 {
        return Err((-32602, "hash must be 32 bytes".to_string()));
    }
    Ok(H256::from_slice(&bytes))
}

fn parse_hex_address(s: &str) -> Result<EvmAddress, (i64, String)> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| (-32602, format!("invalid address hex: {}", e)))?;
    if bytes.len() != 20 {
        return Err((-32602, "address must be 20 bytes".to_string()));
    }
    Ok(EvmAddress::from_slice(&bytes))
}

fn h256_to_hex(h: &H256) -> String {
    format!("0x{}", hex::encode(h.as_bytes()))
}

fn address_to_hex(a: &EvmAddress) -> String {
    format!("0x{}", hex::encode(a.as_bytes()))
}

fn u256_to_hex(u: &U256) -> String {
    format!("0x{}", u.to_string().trim_start_matches("0x"))
}

fn block_json(
    requested: u64,
    transactions: Vec<H256>,
    gas_used: u64,
) -> Value {
    let hash = block_hash_for(requested);
    let parent_hash = if requested == 0 {
        H256::zero()
    } else {
        block_hash_for(requested - 1)
    };

    let empty_hash = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let empty_addr = "0x0000000000000000000000000000000000000000";
    let empty_logs = "0x".to_string() + &"0".repeat(512);

    Value::Object(serde_json::Map::from_iter([
        ("number".to_string(), Value::String(format!("0x{:x}", requested))),
        ("hash".to_string(), Value::String(h256_to_hex(&hash))),
        (
            "parentHash".to_string(),
            Value::String(h256_to_hex(&parent_hash)),
        ),
        ("sha3Uncles".to_string(), Value::String(empty_hash.to_string())),
        ("miner".to_string(), Value::String(empty_addr.to_string())),
        ("stateRoot".to_string(), Value::String(empty_hash.to_string())),
        (
            "transactionsRoot".to_string(),
            Value::String(empty_hash.to_string()),
        ),
        (
            "receiptsRoot".to_string(),
            Value::String(empty_hash.to_string()),
        ),
        ("logsBloom".to_string(), Value::String(empty_logs)),
        ("difficulty".to_string(), Value::String("0x0".to_string())),
        (
            "totalDifficulty".to_string(),
            Value::String("0x0".to_string()),
        ),
        ("extraData".to_string(), Value::String("0x".to_string())),
        ("size".to_string(), Value::String("0x0".to_string())),
        ("gasLimit".to_string(), Value::String("0x1c9c380".to_string())),
        ("gasUsed".to_string(), Value::String(format!("0x{:x}", gas_used))),
        (
            "timestamp".to_string(),
            Value::String(format!(
                "0x{:x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            )),
        ),
        ("mixHash".to_string(), Value::String(empty_hash.to_string())),
        ("nonce".to_string(), Value::String("0x0000000000000000".to_string())),
        (
            "transactions".to_string(),
            Value::Array(transactions.iter().map(h256_to_hex).map(Value::String).collect()),
        ),
    ]))
}

async fn get_block_by_number(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let tag = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing block number".to_string()))?;

    let tick = state
        .oscillator
        .synthesis_tick
        .load(std::sync::atomic::Ordering::SeqCst);
    let requested = match tag {
        "latest" | "pending" | "safe" | "finalized" => tick,
        _ => u64::from_str_radix(tag.trim_start_matches("0x"), 16)
            .map_err(|e| (-32602, format!("invalid block number: {}", e)))?,
    };

    if requested > tick {
        return Ok(Value::Null);
    }

    let (txs, gas_used) = {
        let pool = state.oscillator.evm_pool.lock().unwrap();
        let txs: Vec<H256> = pool
            .receipts
            .values()
            .filter(|r| r.block_number == requested)
            .map(|r| r.transaction_hash)
            .collect();
        let gas_used: u64 = pool
            .receipts
            .values()
            .filter(|r| r.block_number == requested)
            .map(|r| r.gas_used)
            .sum();
        (txs, gas_used)
    };

    Ok(block_json(requested, txs, gas_used))
}

async fn get_block_by_hash(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let hash_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing block hash".to_string()))?;
    let requested_hash = parse_hex_hash(hash_hex)?;

    let tick = state
        .oscillator
        .synthesis_tick
        .load(std::sync::atomic::Ordering::SeqCst);

    // Block hashes are deterministic from the tick, so walk backwards until we
    // find a match.
    let mut requested = None;
    for n in (0..=tick).rev() {
        if block_hash_for(n) == requested_hash {
            requested = Some(n);
            break;
        }
    }

    let Some(requested) = requested else {
        return Ok(Value::Null);
    };

    let (txs, gas_used) = {
        let pool = state.oscillator.evm_pool.lock().unwrap();
        let txs: Vec<H256> = pool
            .receipts
            .values()
            .filter(|r| r.block_number == requested)
            .map(|r| r.transaction_hash)
            .collect();
        let gas_used: u64 = pool
            .receipts
            .values()
            .filter(|r| r.block_number == requested)
            .map(|r| r.gas_used)
            .sum();
        (txs, gas_used)
    };

    Ok(block_json(requested, txs, gas_used))
}

async fn chain_id() -> Value {
    Value::String(format!("0x{:x}", FLUIDIC_CHAIN_ID))
}

async fn get_balance(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let address = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing address parameter".to_string()))?;
    let addr_bytes = hex::decode(address.trim_start_matches("0x"))
        .map_err(|e| (-32602, format!("invalid address hex: {}", e)))?;
    if addr_bytes.len() != 20 {
        return Err((-32602, "address must be 20 bytes".to_string()));
    }

    // Derive a Fluidic account deterministically from the EVM address.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:evm-account:v1");
    hasher.update(&addr_bytes);
    let mut account = [0u8; 32];
    account.copy_from_slice(hasher.finalize().as_bytes());

    let balance = state
        .oscillator
        .wave_field
        .lock()
        .unwrap()
        .account_balance(crate::crypto::AccountId(account))
        .units;
    Ok(Value::String(format!("0x{:x}", balance)))
}

async fn send_raw_transaction(
    state: Arc<ApiState>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    let raw_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing raw transaction".to_string()))?;
    let raw = hex::decode(raw_hex.trim_start_matches("0x"))
        .map_err(|e| (-32602, format!("invalid hex: {}", e)))?;

    let tx = EvmTransaction::decode_raw(&raw)
        .map_err(|e| (-32000, format!("transaction rejected: {}", e)))?;
    let hash = tx.hash;
    let from_hex = format!("0x{}", hex::encode(tx.from.as_bytes()));
    let to_hex = tx.to.map(|addr| format!("0x{}", hex::encode(addr.as_bytes())));
    let value_str = tx.value.to_string();

    state
        .oscillator
        .evm_pool
        .lock()
        .unwrap()
        .submit(tx)
        .map_err(|e| (-32000, e))?;

    let hash_hex = format!("0x{}", hex::encode(tx_hash_bytes(&hash)));
    state.record_shift(RecentShift {
        hash: hash_hex.clone(),
        kind: "evm".to_string(),
        status: "accepted".to_string(),
        domain: None,
        from: Some(from_hex),
        to: to_hex,
        amount: Some(value_str),
        token: Some("ETH".to_string()),
        timestamp_ns: 0,
    });

    Ok(Value::String(hash_hex))
}

fn transaction_to_json(tx: &EvmTransaction, block_number: Option<u64>) -> Value {
    let block_hash = block_number.map(block_hash_for).map(|h| h256_to_hex(&h));
    let mut obj = serde_json::Map::new();
    obj.insert("hash".to_string(), Value::String(h256_to_hex(&tx.hash)));
    obj.insert("from".to_string(), Value::String(address_to_hex(&tx.from)));
    obj.insert(
        "to".to_string(),
        tx.to.as_ref().map(address_to_hex).map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert("value".to_string(), Value::String(u256_to_hex(&tx.value)));
    obj.insert("gas".to_string(), Value::String(format!("0x{:x}", tx.gas_limit)));
    obj.insert(
        "gasPrice".to_string(),
        Value::String(u256_to_hex(&tx.gas_price)),
    );
    obj.insert("nonce".to_string(), Value::String(format!("0x{:x}", tx.nonce)));
    obj.insert("input".to_string(), Value::String(format!("0x{}", hex::encode(&tx.data))));
    obj.insert("v".to_string(), Value::String("0x0".to_string()));
    obj.insert("r".to_string(), Value::String("0x0".to_string()));
    obj.insert("s".to_string(), Value::String("0x0".to_string()));
    obj.insert("chainId".to_string(), Value::String(format!("0x{:x}", FLUIDIC_CHAIN_ID)));
    obj.insert(
        "blockNumber".to_string(),
        block_number
            .map(|n| Value::String(format!("0x{:x}", n)))
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "blockHash".to_string(),
        block_hash.map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert("transactionIndex".to_string(), Value::String("0x0".to_string()));
    Value::Object(obj)
}

async fn get_transaction_by_hash(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let hash_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing transaction hash".to_string()))?;
    let hash = parse_hex_hash(hash_hex)?;

    let pool = state.oscillator.evm_pool.lock().unwrap();
    if let Some(tx) = pool.transaction(&hash) {
        let block_number = pool.receipt(&hash).map(|r| r.block_number);
        return Ok(transaction_to_json(tx, block_number));
    }
    Ok(Value::Null)
}

fn receipt_to_json(receipt: &crate::evm::EvmReceipt) -> Value {
    let logs = receipt
        .logs
        .iter()
        .enumerate()
        .map(|(i, log)| {
            Value::Object(serde_json::Map::from_iter([
                ("logIndex".to_string(), Value::String(format!("0x{:x}", i))),
                (
                    "transactionIndex".to_string(),
                    Value::String(format!("0x{:x}", receipt.transaction_index)),
                ),
                (
                    "transactionHash".to_string(),
                    Value::String(h256_to_hex(&receipt.transaction_hash)),
                ),
                (
                    "blockHash".to_string(),
                    Value::String(h256_to_hex(&receipt.block_hash)),
                ),
                (
                    "blockNumber".to_string(),
                    Value::String(format!("0x{:x}", receipt.block_number)),
                ),
                ("address".to_string(), Value::String(address_to_hex(&log.address))),
                (
                    "topics".to_string(),
                    Value::Array(log.topics.iter().map(h256_to_hex).map(Value::String).collect()),
                ),
                ("data".to_string(), Value::String(format!("0x{}", hex::encode(&log.data)))),
                ("removed".to_string(), Value::Bool(false)),
            ]))
        })
        .collect();

    Value::Object(serde_json::Map::from_iter([
        (
            "transactionHash".to_string(),
            Value::String(h256_to_hex(&receipt.transaction_hash)),
        ),
        (
            "transactionIndex".to_string(),
            Value::String(format!("0x{:x}", receipt.transaction_index)),
        ),
        (
            "blockHash".to_string(),
            Value::String(h256_to_hex(&receipt.block_hash)),
        ),
        (
            "blockNumber".to_string(),
            Value::String(format!("0x{:x}", receipt.block_number)),
        ),
        ("from".to_string(), Value::String(address_to_hex(&receipt.from))),
        (
            "to".to_string(),
            receipt
                .to
                .as_ref()
                .map(address_to_hex)
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "contractAddress".to_string(),
            receipt
                .contract_address
                .as_ref()
                .map(address_to_hex)
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "cumulativeGasUsed".to_string(),
            Value::String(format!("0x{:x}", receipt.cumulative_gas_used)),
        ),
        ("gasUsed".to_string(), Value::String(format!("0x{:x}", receipt.gas_used))),
        (
            "effectiveGasPrice".to_string(),
            Value::String(u256_to_hex(&receipt.effective_gas_price)),
        ),
        ("status".to_string(), Value::String(format!("0x{:x}", receipt.status))),
        ("logs".to_string(), Value::Array(logs)),
        ("logsBloom".to_string(), Value::String("0x".to_string() + &"0".repeat(512))),
    ]))
}

async fn get_transaction_receipt(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let hash_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing transaction hash".to_string()))?;
    let hash = parse_hex_hash(hash_hex)?;

    let pool = state.oscillator.evm_pool.lock().unwrap();
    if let Some(receipt) = pool.receipt(&hash) {
        return Ok(receipt_to_json(receipt));
    }

    match pool.status(&hash) {
        Some(crate::evm::EvmTxStatus::Pending) => Ok(Value::Null),
        Some(crate::evm::EvmTxStatus::Failed(reason)) => Ok(Value::Object(
            serde_json::Map::from_iter([
                ("transactionHash".to_string(), Value::String(hash_hex.to_string())),
                ("status".to_string(), Value::String("0x0".to_string())),
                ("gasUsed".to_string(), Value::String("0x0".to_string())),
                ("revertReason".to_string(), Value::String(reason)),
            ]),
        )),
        _ => Ok(Value::Null),
    }
}

async fn get_transaction_count(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let address = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing address".to_string()))?;
    let addr = parse_hex_address(address)?;
    let nonce = state.oscillator.evm_pool.lock().unwrap().nonce(&addr);
    Ok(Value::String(format!("0x{:x}", nonce)))
}

async fn get_code(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let address = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing address".to_string()))?;
    let addr = parse_hex_address(address)?;
    let pool = state.oscillator.evm_pool.lock().unwrap();
    let executor = crate::evm::EvmExecutor::with_db(pool.db.clone());
    let code = executor.code_at(addr).unwrap_or_default();
    Ok(Value::String(format!("0x{}", hex::encode(code))))
}

async fn eth_call(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let call = params
        .first()
        .and_then(|v| v.as_object())
        .ok_or((-32602, "missing call object".to_string()))?;

    let from = call
        .get("from")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<EvmAddress>().ok())
        .unwrap_or_else(|| EvmAddress::zero());
    let to = call
        .get("to")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<EvmAddress>().ok());
    let data = call
        .get("data")
        .and_then(|v| v.as_str())
        .map(|s| hex::decode(s.trim_start_matches("0x")).unwrap_or_default())
        .unwrap_or_default();
    let value = call
        .get("value")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<U256>().ok())
        .unwrap_or_default();

    let pool = state.oscillator.evm_pool.lock().unwrap();
    let mut executor = crate::evm::EvmExecutor::with_db(pool.db.clone());

    // Seed the caller and target with their current Fluidic balances so the
    // call sees the same state as a committed transaction would.
    let wave = state.oscillator.wave_field.lock().unwrap();
    for addr in [Some(from), to].into_iter().flatten() {
        let fluidic = crate::evm::evm_address_to_fluidic(&addr);
        let balance = wave.account_balance(fluidic).units;
        let nonce = pool.nonces.get(&addr).copied().unwrap_or(0);
        executor.seed_balance_nonce(addr, balance, nonce);
    }
    drop(wave);
    drop(pool);

    match executor.call(from, to, value, data) {
        Ok(result) => match result {
            revm::primitives::ExecutionResult::Success { output, .. } => {
                let bytes = output.data();
                Ok(Value::String(format!("0x{}", hex::encode(bytes))))
            }
            revm::primitives::ExecutionResult::Revert { output, .. } => Err((
                -32000,
                format!("execution reverted: 0x{}", hex::encode(output)),
            )),
            revm::primitives::ExecutionResult::Halt { reason, .. } => Err((
                -32000,
                format!("execution halted: {:?}", reason),
            )),
        },
        Err(e) => Err((-32000, format!("execution failed: {:?}", e))),
    }
}

async fn estimate_gas(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let call = params
        .first()
        .and_then(|v| v.as_object())
        .ok_or((-32602, "missing call object".to_string()))?;

    let from = call
        .get("from")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<EvmAddress>().ok())
        .unwrap_or_else(|| EvmAddress::zero());
    let to = call
        .get("to")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<EvmAddress>().ok());
    let data = call
        .get("data")
        .and_then(|v| v.as_str())
        .map(|s| hex::decode(s.trim_start_matches("0x")).unwrap_or_default())
        .unwrap_or_default();
    let value = call
        .get("value")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<U256>().ok())
        .unwrap_or_default();

    let pool = state.oscillator.evm_pool.lock().unwrap();
    let mut executor = crate::evm::EvmExecutor::with_db(pool.db.clone());
    let wave = state.oscillator.wave_field.lock().unwrap();
    for addr in [Some(from), to].into_iter().flatten() {
        let fluidic = crate::evm::evm_address_to_fluidic(&addr);
        let balance = wave.account_balance(fluidic).units;
        let nonce = pool.nonces.get(&addr).copied().unwrap_or(0);
        executor.seed_balance_nonce(addr, balance, nonce);
    }
    drop(wave);
    drop(pool);

    match executor.call(from, to, value, data) {
        Ok(result) => {
            let gas = revm::primitives::ExecutionResult::gas_used(&result);
            Ok(Value::String(format!("0x{:x}", gas)))
        }
        Err(e) => Err((-32000, format!("execution failed: {:?}", e))),
    }
}

async fn get_logs(
    state: Arc<ApiState>,
    min_tick: Option<u64>,
    params: Vec<Value>,
) -> Result<Value, (i64, String)> {
    wait_for_min_tick(&state, min_tick).await;
    let filter = params
        .first()
        .and_then(|v| v.as_object())
        .ok_or((-32602, "missing filter object".to_string()))?;

    let address = filter
        .get("address")
        .and_then(|v| v.as_str())
        .map(parse_hex_address)
        .transpose()?;

    let mut topics = Vec::new();
    if let Some(arr) = filter.get("topics").and_then(|v| v.as_array()) {
        for t in arr {
            if t.is_null() {
                topics.push(None);
            } else if let Some(s) = t.as_str() {
                topics.push(Some(parse_hex_hash(s)?));
            }
        }
    }

    let logs = state
        .oscillator
        .evm_pool
        .lock()
        .unwrap()
        .logs(address, &topics);

    let out: Vec<Value> = logs
        .iter()
        .map(|log| {
            Value::Object(serde_json::Map::from_iter([
                ("address".to_string(), Value::String(address_to_hex(&log.address))),
                (
                    "topics".to_string(),
                    Value::Array(log.topics.iter().map(h256_to_hex).map(Value::String).collect()),
                ),
                ("data".to_string(), Value::String(format!("0x{}", hex::encode(&log.data)))),
            ]))
        })
        .collect();

    Ok(Value::Array(out))
}

fn tx_hash_bytes(hash: &H256) -> [u8; 32] {
    hash.to_fixed_bytes()
}

/// EVM-style debug namespace endpoint used by some explorers.
pub async fn rpc_health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::state::ApiState;
    use crate::consensus::Oscillator;
    use crate::crypto::AccountId;
    use std::sync::Arc;

    #[tokio::test]
    async fn chain_id_is_constant() {
        assert_eq!(chain_id().await, Value::String("0xf1d1c".to_string()));
    }

    #[tokio::test]
    async fn block_number_reflects_synthesis_tick() {
        let state = Arc::new(ApiState::new(Arc::new(Oscillator::new([0u8; 32], 512))));
        state.oscillator.synthesis_tick.store(42, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(block_number(state).await, Value::String("0x2a".to_string()));
    }

    #[tokio::test]
    async fn get_balance_derives_account_from_evm_address() {
        let osc = Arc::new(Oscillator::new([0u8; 32], 512));
        let state = Arc::new(ApiState::new(osc.clone()));

        let evm_address = "0000000000000000000000000000000000000001";
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fluidic:evm-account:v1");
        hasher.update(&hex::decode(evm_address).unwrap());
        let mut account = [0u8; 32];
        account.copy_from_slice(hasher.finalize().as_bytes());
        osc.seed_account(AccountId(account), 5_000_000_000_000);

        let balance = get_balance(state, None, vec![Value::String(format!("0x{}", evm_address))])
            .await
            .unwrap();
        assert_eq!(balance, Value::String("0x48c27395000".to_string()));
    }
}
