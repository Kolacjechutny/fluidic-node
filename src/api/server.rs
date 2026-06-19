use crate::api::evm_rpc::evm_rpc_router;
use crate::api::routes::api_router;
use crate::api::state::ApiState;
use crate::api::websocket::start_broadcast_task;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

pub async fn start_api_server(state: Arc<ApiState>, port: u16) -> Result<(), String> {
    start_broadcast_task(state.clone());

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app: Router = api_router()
        .merge(evm_rpc_router())
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", port)
        .parse()
        .map_err(|e| format!("invalid API address: {}", e))?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("failed to bind API server: {}", e))?;

    tracing::info!("API server listening on http://{}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| format!("API server error: {}", e))
}
