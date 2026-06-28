use crate::api::evm_rpc::evm_rpc_router;
use crate::api::routes::api_router;
use crate::api::state::ApiState;
use crate::api::websocket::start_broadcast_task;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware::{Next, from_fn_with_state},
    response::IntoResponse,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Global token-bucket state shared by the rate-limit middleware.
#[derive(Clone)]
struct RateLimitState {
    bucket: Arc<Mutex<TokenBucket>>,
}

struct TokenBucket {
    tokens: f64,
    last_update: Instant,
    rate_per_sec: f64,
    capacity: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_update: Instant::now(),
            rate_per_sec,
            capacity,
        }
    }

    /// Try to consume one token. Returns true if allowed.
    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        self.last_update = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

async fn rate_limit_middleware(
    State(state): State<RateLimitState>,
    request: axum::extract::Request,
    next: Next,
) -> impl IntoResponse {
    let allowed = state.bucket.lock().map(|mut b| b.allow()).unwrap_or(true);
    if allowed {
        next.run(request).await
    } else {
        (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "rate limit exceeded" })),
        )
            .into_response()
    }
}

pub async fn start_api_server(state: Arc<ApiState>, port: u16) -> Result<(), String> {
    start_broadcast_task(state.clone());

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let rate_limit_state = RateLimitState {
        bucket: Arc::new(Mutex::new(TokenBucket::new(300.0, 600.0))),
    };

    let app: Router = api_router()
        .merge(evm_rpc_router())
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(256 * 1024))
        .layer(from_fn_with_state(rate_limit_state, rate_limit_middleware))
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
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
