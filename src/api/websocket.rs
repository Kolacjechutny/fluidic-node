use crate::api::state::ApiState;
use axum::extract::ws::{Message, WebSocket};
use std::sync::Arc;

pub async fn handle_socket(mut socket: WebSocket, state: Arc<ApiState>) {
    let mut rx = state.ws_tx.subscribe();

    // Send initial snapshot immediately.
    let initial = serde_json::to_string(&snapshot_json(&state)).unwrap_or_default();
    let _ = socket.send(Message::Text(initial)).await;

    loop {
        tokio::select! {
            Ok(snap) = rx.recv() => {
                let msg = serde_json::to_string(&snapshot_json_from_snap(snap, &state)).unwrap_or_default();
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                if socket.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn snapshot_json(state: &ApiState) -> serde_json::Value {
    let snap = state.snapshot();
    snapshot_json_from_snap(snap, state)
}

fn snapshot_json_from_snap(
    snap: crate::api::state::StateSnapshot,
    state: &ApiState,
) -> serde_json::Value {
    serde_json::json!({
        "wave_reserve": snap.wave_reserve.to_string(),
        "usdc_reserve": snap.usdc_reserve.to_string(),
        "price": snap.price,
        "throughput": snap.throughput,
        "latency_ms": snap.latency_ms,
        "network_ms": snap.network_ms,
        "metabolic_burned": snap.metabolic_burned.to_string(),
        "commutative_applied": snap.commutative_applied,
        "stateful_applied": snap.stateful_applied,
        "evm_applied": snap.evm_applied,
        "pool_wave_account": hex::encode(state.pool_wave_account.0),
        "pool_usdc_account": hex::encode(state.pool_usdc_account.0),
    })
}

pub fn start_broadcast_task(state: Arc<ApiState>) {
    tokio::spawn(async move {
        let mut last = state.snapshot();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let snap = state.snapshot();
            if snap.price != last.price
                || snap.wave_reserve != last.wave_reserve
                || snap.usdc_reserve != last.usdc_reserve
                || snap.commutative_applied != last.commutative_applied
                || snap.stateful_applied != last.stateful_applied
                || snap.evm_applied != last.evm_applied
                || (snap.throughput - last.throughput).abs() > f64::EPSILON
                || (snap.latency_ms - last.latency_ms).abs() > f64::EPSILON
            {
                let _ = state.ws_tx.send(snap.clone());
                last = snap;
            }
        }
    });
}
