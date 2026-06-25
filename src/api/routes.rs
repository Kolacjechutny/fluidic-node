use crate::api::state::{ApiState, RecentShift, build_pool_payout_shift};
use crate::consensus::dag::{DagError, ShiftStatus, VectorClockDag};
use crate::crypto::{AccountId, CommutativeShift, KeyPair, Signal, StakeShift, StatefulShift};
use crate::evm::evm_address_to_fluidic;
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub fn api_router() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/state", get(get_state))
        .route("/api/account/:id/balance", get(get_balance))
        .route("/api/account/register", post(register_account))
        .route("/api/shift/commutative", post(submit_commutative))
        .route("/api/shift/stateful", post(submit_stateful))
        .route("/api/shifts/recent", get(get_recent_shifts))
        .route("/api/shift/:hash/status", get(shift_status))
        .route("/api/certificate/:tick", get(get_certificate))
        .route("/api/quorum/:tick", get(get_quorum_status))
        .route("/api/ticks/recent", get(get_recent_ticks))
        .route("/api/ticks/:tick", get(get_tick))
        .route("/api/operator/info", get(get_operator_info))
        .route("/api/operator/stake", post(submit_operator_stake))
        .route("/api/operators", get(get_staked_operators))
        .route("/api/operator/:id/rewards", get(get_operator_rewards))
        .route("/api/evm/faucet", post(evm_faucet))
        .route("/api/ws", get(ws_handler))
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Serialize)]
struct StateResponse {
    wave_reserve: String,
    usdc_reserve: String,
    price: f64,
    throughput: f64,
    latency_ms: f64,
    metabolic_burned: String,
    commutative_applied: usize,
    stateful_applied: usize,
    evm_applied: usize,
    pool_wave_account: String,
    pool_usdc_account: String,
}

#[derive(Deserialize)]
struct StateQuery {
    #[serde(default)]
    min_tick: Option<u64>,
}

#[derive(Deserialize)]
struct CommutativeShiftRequest {
    #[serde(default)]
    domain: Option<String>,
    coordinate: CoordinateRequest,
    delta: String,
    pool_id: String,
    nonce: u64,
    timestamp_ns: u64,
    signature: String,
}

#[derive(Deserialize)]
struct CoordinateRequest {
    components: Vec<u64>,
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

async fn get_state(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;

    let snap = state.snapshot();
    Json(StateResponse {
        wave_reserve: snap.wave_reserve.to_string(),
        usdc_reserve: snap.usdc_reserve.to_string(),
        price: snap.price,
        throughput: snap.throughput,
        latency_ms: snap.latency_ms,
        metabolic_burned: snap.metabolic_burned.to_string(),
        commutative_applied: snap.commutative_applied,
        stateful_applied: snap.stateful_applied,
        evm_applied: snap.evm_applied,
        pool_wave_account: hex::encode(state.pool_wave_account.0),
        pool_usdc_account: hex::encode(state.pool_usdc_account.0),
    })
}

#[derive(Serialize)]
struct BalanceResponse {
    wave: String,
    usdc: String,
}

async fn get_balance(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(query): Query<StateQuery>,
) -> Result<Json<BalanceResponse>, StatusCode> {
    wait_for_min_tick(&state, query.min_tick).await;

    let bytes = hex::decode(&id).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 32 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    let user = AccountId(arr);
    let (wave_acc, usdc_acc) = state.token_accounts(user);

    let field = state.oscillator.wave_field.lock().unwrap();
    let wave = field.account_balance(wave_acc).units;
    let usdc = field.account_balance(usdc_acc).units;
    drop(field);

    Ok(Json(BalanceResponse {
        wave: wave.to_string(),
        usdc: usdc.to_string(),
    }))
}

#[derive(Deserialize)]
struct RegisterRequest {
    public_key_hex: String,
}

async fn register_account(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pk_bytes = hex::decode(&req.public_key_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    let vk = VerifyingKey::from_bytes(&pk_bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let account = AccountId::from_public_key(&vk);
    state.register_key(account, vk);

    // Faucet: seed both token accounts for the demo.
    let (wave_acc, usdc_acc) = state.token_accounts(account);
    state.oscillator.seed_account(wave_acc, 1_000_000_000_000_000); // 1,000 WAVE
    state.oscillator.seed_account(usdc_acc, 1_000_000_000_000_000); // 1,000 USDC

    // Register derived token accounts so they can sign stateful shifts.
    state.register_key(wave_acc, vk);
    state.register_key(usdc_acc, vk);

    // Map derived token accounts back to the owner main account for payouts.
    state.register_derived(wave_acc, account);
    state.register_derived(usdc_acc, account);

    // Gossip the registration so every mesh node learns this account.
    state.broadcast_registration(crate::crypto::RegistrationShift {
        account,
        public_key: vk.to_bytes(),
        wave_account: wave_acc,
        usdc_account: usdc_acc,
        nonce: 0,
        timestamp_ns: 0,
    });

    Ok(Json(serde_json::json!({
        "account_id": account.to_string(),
        "wave_account": hex::encode(wave_acc.0),
        "usdc_account": hex::encode(usdc_acc.0),
    })))
}

#[derive(Deserialize)]
struct EvmFaucetRequest {
    address: String,
}

async fn evm_faucet(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<EvmFaucetRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let addr_bytes = hex::decode(req.address.trim_start_matches("0x"))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if addr_bytes.len() != 20 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let addr = ethers_core::types::Address::from_slice(&addr_bytes);
    let fluidic_account = evm_address_to_fluidic(&addr);
    state.oscillator.seed_account(fluidic_account, 1_000_000_000_000_000); // 1,000 WAVE
    Ok(Json(serde_json::json!({
        "address": req.address,
        "fluidic_account": fluidic_account.to_string(),
        "dripped_wave": "1000",
    })))
}

#[derive(Deserialize)]
struct VectorClockInput {
    entries: std::collections::HashMap<String, u64>,
}

#[derive(Deserialize)]
struct StatefulShiftRequest {
    from: String,
    to: String,
    amount: String,
    #[serde(default)]
    domain: Option<crate::crypto::DomainId>,
    #[serde(default)]
    vector_clock: Option<VectorClockInput>,
    predecessors: Vec<String>,
    nonce: u64,
    timestamp_ns: u64,
    signature: String,
}

fn parse_account(hex: &str) -> Result<AccountId, StatusCode> {
    let bytes = hex::decode(hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 32 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(AccountId(arr))
}

fn parse_domain(hex: &str) -> Result<[u8; 32], StatusCode> {
    parse_hash(hex)
}

fn parse_hash(hex: &str) -> Result<[u8; 32], StatusCode> {
    let bytes = hex::decode(hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 32 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn parse_u128(s: &str) -> Result<u128, StatusCode> {
    s.parse().map_err(|_| StatusCode::BAD_REQUEST)
}

fn parse_stateful_shift(req: StatefulShiftRequest) -> Result<StatefulShift, (StatusCode, String)> {
    let from = parse_account(&req.from).map_err(|e| (e, "invalid from account".to_string()))?;
    let to = parse_account(&req.to).map_err(|e| (e, "invalid to account".to_string()))?;
    let amount = parse_u128(&req.amount).map_err(|e| (e, "invalid amount".to_string()))?;
    let signature = hex::decode(&req.signature)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid signature hex".to_string()))?;

    let clock_map = req.vector_clock
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing vector_clock".to_string()))?
        .entries;
    let mut vector_clock = crate::crypto::VectorClock::new();
    for (node_hex, time) in clock_map {
        let node = parse_hash(&node_hex).map_err(|e| (e, "invalid vector_clock node".to_string()))?;
        vector_clock.0.insert(node, time);
    }

    let predecessors = req
        .predecessors
        .into_iter()
        .map(|h| parse_hash(&h).map_err(|e| (e, "invalid predecessor".to_string())))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StatefulShift {
        domain: req.domain.unwrap_or(crate::crypto::DEFAULT_DEX_DOMAIN),
        from,
        to,
        amount,
        vector_clock,
        predecessors,
        nonce: req.nonce,
        timestamp_ns: req.timestamp_ns,
        first_seen_at_ns: 0,
        signature,
    })
}

async fn submit_stateful(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<StatefulShiftRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let shift = parse_stateful_shift(req)?;

    let registry = state.registry.read().unwrap();
    let pk = match registry.get(&shift.from) {
        Some(pk) => pk,
        None => {
            tracing::warn!("stateful shift rejected: unknown sender {}", shift.from);
            return Err((StatusCode::UNAUTHORIZED, "unknown sender".to_string()));
        }
    };
    let sig = Signature::from_slice(&shift.signature)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid signature bytes".to_string()))?;
    if !KeyPair::verify(pk, &shift.signing_bytes(), &sig) {
        tracing::warn!(
            "stateful shift rejected: invalid signature from {}",
            shift.from
        );
        return Err((StatusCode::UNAUTHORIZED, "invalid signature".to_string()));
    }
    drop(registry);

    // Validate vector clock against locally observed causal history.
    {
        let dag = state.oscillator.dag.lock().unwrap();
        if let Err(e) = dag.validate_vector_clock(shift.from, &shift.vector_clock) {
            tracing::warn!(
                "stateful shift rejected: invalid vector clock from {}: {}",
                shift.from,
                e
            );
            return Err((StatusCode::BAD_REQUEST, e));
        }
    }

    // If the shift targets a pool, create a matching payout.
    let is_wave_to_pool = shift.to == state.pool_wave_account;
    let is_usdc_to_pool = shift.to == state.pool_usdc_account;

    if is_wave_to_pool || is_usdc_to_pool {
        let main_account = state.main_account(shift.from)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "derived account not registered".to_string()))?;
        let (wave_user, usdc_user) = state.token_accounts(main_account);

        let (payout_from, payout_to, payout_amount) = if is_wave_to_pool {
            let out = state.wave_to_usdc_out(shift.amount);
            if out == 0 {
                return Err((StatusCode::SERVICE_UNAVAILABLE, "swap output is zero".to_string()));
            }
            (state.pool_usdc_account, usdc_user, out)
        } else {
            let out = state.usdc_to_wave_out(shift.amount);
            if out == 0 {
                return Err((StatusCode::SERVICE_UNAVAILABLE, "swap output is zero".to_string()));
            }
            (state.pool_wave_account, wave_user, out)
        };

        let payout = {
            let dag = state.oscillator.dag.lock().unwrap();
            let tip = dag.account_tip(&payout_from);
            build_pool_payout_shift(&state.pool_keypair, payout_from, payout_to, payout_amount, shift.nonce, &tip)
        };
        state
            .oscillator
            .ingest(Signal::Stateful(payout))
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("payout ingest failed: {}", e)))?;
    }

    let user_hash = shift.hash();
    state.record_shift(RecentShift {
        hash: hex::encode(user_hash),
        kind: "stateful".to_string(),
        status: "accepted".to_string(),
        domain: Some(hex::encode(shift.domain)),
        from: Some(shift.from.to_string()),
        to: Some(shift.to.to_string()),
        amount: Some(shift.amount.to_string()),
        timestamp_ns: shift.timestamp_ns,
    });
    state
        .oscillator
        .ingest(Signal::Stateful(shift))
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("shift ingest failed: {}", e)))?;

    Ok(Json(serde_json::json!({
        "status": "queued",
        "hash": hex::encode(user_hash)
    })))
}

async fn submit_commutative(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<CommutativeShiftRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let domain = match req.domain {
        Some(hex) => parse_domain(&hex)?,
        None => crate::crypto::DEFAULT_DEX_DOMAIN,
    };
    let components: [u64; 4] = req
        .coordinate
        .components
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let delta = req
        .delta
        .parse::<i128>()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let pool_id = parse_hash(&req.pool_id)?;
    let signature = hex::decode(&req.signature).map_err(|_| StatusCode::BAD_REQUEST)?;

    let shift = CommutativeShift {
        domain,
        coordinate: crate::field::coordinates::Coordinate::new(components),
        delta,
        pool_id,
        nonce: req.nonce,
        timestamp_ns: req.timestamp_ns,
        first_seen_at_ns: 0,
        signature,
    };

    let hash = shift.hash();
    state.record_shift(RecentShift {
        hash: hex::encode(hash),
        kind: "commutative".to_string(),
        status: "accepted".to_string(),
        domain: Some(hex::encode(shift.domain)),
        from: None,
        to: None,
        amount: Some(shift.delta.to_string()),
        timestamp_ns: shift.timestamp_ns,
    });
    state
        .oscillator
        .ingest(Signal::Commutative(shift))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(serde_json::json!({
        "hash": hex::encode(hash),
        "status": "queued"
    })))
}

async fn get_operator_info(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let guard = state.operator_keypair.lock().unwrap();
    match guard.as_ref() {
        Some(kp) => {
            let account = kp.account_id();
            let stake = state.oscillator.stake_table.get_stake(&account);
            let min_stake = state.oscillator.stake_table.min_stake();
            Json(serde_json::json!({
                "account": account.to_string(),
                "public_key": hex::encode(kp.public_key().to_bytes()),
                "stake": stake.to_string(),
                "min_stake": min_stake.to_string(),
                "is_staked": state.oscillator.stake_table.is_staked(&account),
            }))
            .into_response()
        }
        None => (StatusCode::SERVICE_UNAVAILABLE, "operator keypair not configured").into_response(),
    }
}

#[derive(Deserialize)]
struct StakeRequest {
    amount: String,
}

async fn submit_operator_stake(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<StakeRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let amount = req
        .amount
        .parse::<u128>()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let kp = state
        .operator_keypair
        .lock()
        .unwrap()
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let stake = StakeShift::sign(&kp, amount, 0, timestamp_ns);

    state.oscillator.apply_stake(&stake);
    state.broadcast_stake(stake.clone());

    Ok(Json(serde_json::json!({
        "status": "staked",
        "operator": kp.account_id().to_string(),
        "amount": amount.to_string(),
        "is_staked": state.oscillator.stake_table.is_staked(&kp.account_id()),
    })))
}

async fn get_staked_operators(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let operators: Vec<_> = state
        .oscillator
        .stake_table
        .staked_operators()
        .into_iter()
        .map(|(account, stake)| {
            serde_json::json!({
                "account": account.to_string(),
                "stake": stake.to_string(),
            })
        })
        .collect();
    Json(serde_json::json!({ "operators": operators }))
}

async fn get_quorum_status(
    State(state): State<Arc<ApiState>>,
    Path(tick): Path<u64>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let threshold = state.oscillator.stake_table.quorum_threshold();
    let total = state.oscillator.stake_table.total_stake();
    match state.oscillator.check_quorum(tick) {
        Some((view, stake)) => Json(serde_json::json!({
            "tick": tick,
            "finalized": true,
            "stake": stake.to_string(),
            "threshold": threshold.to_string(),
            "total_stake": total.to_string(),
            "roots": {
                "commutative": hex::encode(view.commutative_root),
                "stateful": hex::encode(view.stateful_root),
                "evm": hex::encode(view.evm_root),
                "balances": hex::encode(view.balances_root),
                "stake": hex::encode(view.stake_root),
                "reward": hex::encode(view.reward_root),
            }
        })),
        None => Json(serde_json::json!({
            "tick": tick,
            "finalized": false,
            "threshold": threshold.to_string(),
            "total_stake": total.to_string(),
        })),
    }
}

async fn get_certificate(
    State(state): State<Arc<ApiState>>,
    Path(tick): Path<u64>,
    Query(query): Query<StateQuery>,
) -> Result<Json<crate::consensus::certificate::SynthesisCertificate>, StatusCode> {
    wait_for_min_tick(&state, query.min_tick).await;
    let certs = state.oscillator.certificates.read().unwrap();
    certs
        .get(&tick)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

async fn get_operator_rewards(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(query): Query<StateQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    wait_for_min_tick(&state, query.min_tick).await;
    let bytes = hex::decode(&id).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 32 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    let account = AccountId(arr);
    let balance = state.oscillator.reward_pool.read().unwrap().balance(&account);
    Ok(Json(serde_json::json!({
        "account": id,
        "rewards": balance.to_string(),
    })))
}

async fn get_recent_shifts(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let shifts = state.recent_shifts.lock().unwrap().clone();
    Json(serde_json::json!({ "shifts": shifts }))
}

async fn shift_status(
    State(state): State<Arc<ApiState>>,
    Path(hash_hex): Path<String>,
    Query(query): Query<StateQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    wait_for_min_tick(&state, query.min_tick).await;
    let bytes = hex::decode(&hash_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 32 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes);

    let current_tick = state.oscillator.synthesis_tick.load(std::sync::atomic::Ordering::SeqCst);

    let status = match state.shift_status(&hash) {
        Some(ShiftStatus::Accepted) => {
            let inserted = state.oscillator.dag.lock().unwrap()
                .nodes.get(&hash).map(|n| n.inserted_at_tick);
            let confirmations = inserted.map(|t| current_tick.saturating_sub(t)).unwrap_or(0);
            serde_json::json!({
                "hash": hash_hex,
                "status": "accepted",
                "error": null,
                "synthesis_tick": current_tick,
                "confirmations": confirmations,
            })
        }
        Some(ShiftStatus::Finalized) => serde_json::json!({
            "hash": hash_hex,
            "status": "finalized",
            "error": null,
            "synthesis_tick": current_tick,
            "confirmations": VectorClockDag::FINALIZATION_DEPTH,
        }),
        Some(ShiftStatus::Rejected(err)) => serde_json::json!({
            "hash": hash_hex,
            "status": "rejected",
            "error": dag_error_name(&err),
            "synthesis_tick": current_tick,
            "confirmations": 0,
        }),
        None => serde_json::json!({
            "hash": hash_hex,
            "status": "unknown",
            "error": null,
            "synthesis_tick": current_tick,
            "confirmations": 0,
        }),
    };

    Ok(Json(status))
}

fn dag_error_name(err: &DagError) -> &'static str {
    match err {
        DagError::MissingPredecessor(_) => "missing_predecessor",
        DagError::InvalidSignature(_) => "invalid_signature",
        DagError::InsufficientBalance(_) => "insufficient_balance",
        DagError::DoubleSpend(_) => "double_spend",
        DagError::CausalCycle(_) => "causal_cycle",
    }
}

#[derive(Deserialize)]
struct RecentTicksQuery {
    #[serde(default)]
    min_tick: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn get_recent_ticks(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<RecentTicksQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let limit = query.limit.unwrap_or(20).min(100);
    let certs = state.oscillator.certificates.read().unwrap();
    let mut ticks: Vec<_> = certs
        .iter()
        .map(|(tick, cert)| {
            let finalized = state.oscillator.check_quorum(*tick).is_some();
            serde_json::json!({
                "tick": cert.tick,
                "hash": hex::encode(cert.hash()),
                "operator": cert.operator.to_string(),
                "commutative_applied": cert.commutative_applied,
                "stateful_applied": cert.stateful_applied,
                "evm_applied": cert.evm_applied,
                "roots": {
                    "commutative": hex::encode(cert.commutative_root),
                    "stateful": hex::encode(cert.stateful_root),
                    "balances": hex::encode(cert.balances_root),
                    "stake": hex::encode(cert.stake_root),
                    "reward": hex::encode(cert.reward_root),
                },
                "finalized": finalized,
            })
        })
        .collect();
    // Sort descending by tick.
    ticks.sort_by(|a, b| {
        let at = a.get("tick").and_then(|v| v.as_u64()).unwrap_or(0);
        let bt = b.get("tick").and_then(|v| v.as_u64()).unwrap_or(0);
        bt.cmp(&at)
    });
    ticks.truncate(limit);
    Json(serde_json::json!({ "ticks": ticks }))
}

async fn get_tick(
    State(state): State<Arc<ApiState>>,
    Path(tick): Path<u64>,
    Query(query): Query<StateQuery>,
) -> impl IntoResponse {
    wait_for_min_tick(&state, query.min_tick).await;
    let certs = state.oscillator.certificates.read().unwrap();
    match certs.get(&tick) {
        Some(cert) => {
            let finalized = state.oscillator.check_quorum(tick).is_some();
            Json(serde_json::json!({
                "tick": cert.tick,
                "hash": hex::encode(cert.hash()),
                "operator": cert.operator.to_string(),
                "commutative_applied": cert.commutative_applied,
                "stateful_applied": cert.stateful_applied,
                "evm_applied": cert.evm_applied,
                "roots": {
                    "commutative": hex::encode(cert.commutative_root),
                    "stateful": hex::encode(cert.stateful_root),
                    "balances": hex::encode(cert.balances_root),
                    "stake": hex::encode(cert.stake_root),
                    "reward": hex::encode(cert.reward_root),
                },
                "finalized": finalized,
            }))
            .into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "tick not found" }))).into_response(),
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| crate::api::websocket::handle_socket(socket, state))
}
