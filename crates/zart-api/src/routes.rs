//! Route definitions and handler implementations for the Zart HTTP API.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::time::Duration;
use zart::error::SchedulerError;
#[cfg(feature = "metrics")]
use zart::metrics::gather_metrics;

use crate::{
    models::{
        ErrorResponse, ExecutionResponse, ListQuery, StartExecutionRequest, StatsResponse,
        WaitQuery,
    },
    state::AppState,
};

/// Maximum wait allowed by the `wait` endpoint (seconds).
const MAX_WAIT_SECS: u64 = 30;

/// Construct the versioned API router with the given application state.
///
/// Routes are nested under `prefix` (e.g., `"/api/v1"`). Health checks and
/// metrics are mounted at the root, outside the prefix.
pub fn api_router(state: AppState, prefix: &str) -> Router {
    let inner = Router::new()
        // Execution management
        .route("/executions", get(list_executions))
        .route("/executions", post(start_execution))
        .route("/executions/{execution_id}", get(get_execution))
        .route("/executions/{execution_id}/cancel", post(cancel_execution))
        .route("/executions/{execution_id}/wait", get(wait_execution))
        // Stats
        .route("/stats", get(get_stats))
        // Event delivery
        .route("/events/{execution_id}/{event_name}", post(offer_event));

    #[allow(unused_mut)]
    let mut app = Router::new()
        .nest(prefix, inner)
        // Health checks are root-level, unaffected by prefix
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz));

    #[cfg(feature = "metrics")]
    {
        app = app.route("/metrics", get(metrics_handler));
    }

    app.with_state(state)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /healthz` — liveness probe.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "Service is alive"),
    ),
    tag = "health"
))]
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `GET /readyz` — readiness probe (checks if the service is ready to accept requests).
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/readyz",
    responses(
        (status = 200, description = "Service is ready"),
        (status = 503, description = "Service not ready"),
    ),
    tag = "health"
))]
async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    // Check if the durable API is available
    if state.durable.is_ready() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// `GET /metrics` — Prometheus metrics endpoint.
#[cfg(feature = "metrics")]
async fn metrics_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        gather_metrics(),
    )
}

/// `GET /executions` — list executions with optional filters.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/executions",
    params(ListQuery),
    responses(
        (status = 200, description = "List of executions", body = Vec<ExecutionResponse>),
        (status = 500, description = "Internal error",     body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn list_executions(State(state): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let params = q.into_params();

    match state.durable.list_executions(params).await {
        Ok(records) => {
            let body: Vec<ExecutionResponse> = records.into_iter().map(Into::into).collect();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /executions` — start a new durable execution.
///
/// Idempotent: if `executionId` already exists, returns the existing record with 200.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/executions",
    request_body = StartExecutionRequest,
    responses(
        (status = 201, description = "Execution started",                          body = ExecutionResponse),
        (status = 200, description = "Idempotent replay — execution already exists", body = ExecutionResponse),
        (status = 500, description = "Internal error",                             body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn start_execution(
    State(state): State<AppState>,
    Json(req): Json<StartExecutionRequest>,
) -> Response {
    let execution_id = req
        .execution_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    match state
        .durable
        .start(&execution_id, &req.task_name, req.payload)
        .await
    {
        Ok(_) => match state.durable.status(&execution_id).await {
            Ok(record) => {
                let body: ExecutionResponse = record.into();
                (StatusCode::CREATED, Json(body)).into_response()
            }
            Err(e) => scheduler_error_response(e),
        },
        Err(e) => scheduler_error_response(e),
    }
}

/// `GET /executions/:execution_id` — get execution status and step progress.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/executions/{execution_id}",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    responses(
        (status = 200, description = "Execution found",   body = ExecutionResponse),
        (status = 404, description = "Not found",         body = ErrorResponse),
        (status = 500, description = "Internal error",    body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn get_execution(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Response {
    match state.durable.status(&execution_id).await {
        Ok(record) => {
            let body: ExecutionResponse = record.into();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /executions/:execution_id/cancel` — cancel a running execution.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/executions/{execution_id}/cancel",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    responses(
        (status = 204, description = "Cancelled"),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn cancel_execution(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Response {
    match state.durable.cancel(&execution_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `GET /executions/:execution_id/wait` — long-poll until completion.
///
/// Accepts an optional `timeout_secs` query parameter (max 30, default 30).
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/executions/{execution_id}/wait",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
        WaitQuery,
    ),
    responses(
        (status = 200, description = "Execution completed",  body = ExecutionResponse),
        (status = 404, description = "Not found",            body = ErrorResponse),
        (status = 504, description = "Wait timed out",       body = ErrorResponse),
        (status = 500, description = "Internal error",       body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn wait_execution(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Response {
    let secs = q.timeout_secs.unwrap_or(MAX_WAIT_SECS).min(MAX_WAIT_SECS);
    let timeout = Duration::from_secs(secs);

    match state.durable.wait(&execution_id, timeout, None).await {
        Ok(record) => {
            let body: ExecutionResponse = record.into();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(SchedulerError::WaitTimedOut(_)) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(ErrorResponse {
                error: format!("execution '{execution_id}' did not complete within {secs}s"),
            }),
        )
            .into_response(),
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /events/:execution_id/:event_name` — deliver an event.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/events/{execution_id}/{event_name}",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
        ("event_name"   = String, Path, description = "Event name"),
    ),
    request_body = serde_json::Value,
    responses(
        (status = 202, description = "Event accepted"),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "events"
))]
async fn offer_event(
    State(state): State<AppState>,
    Path((execution_id, event_name)): Path<(String, String)>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    match state
        .durable
        .offer_event(&execution_id, &event_name, payload)
        .await
    {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

// ── Stats ──────────────────────────────────────────────────────────────────

/// `GET /stats` — aggregate execution counts by status.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/stats",
    responses(
        (status = 200, description = "Execution statistics", body = StatsResponse),
        (status = 500, description = "Internal error",       body = ErrorResponse),
    ),
    tag = "stats"
))]
async fn get_stats(State(state): State<AppState>) -> Response {
    match state.durable.stats().await {
        Ok(stats) => {
            let body: StatsResponse = stats.into();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => scheduler_error_response(e),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn not_found(execution_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("execution '{execution_id}' not found"),
        }),
    )
        .into_response()
}

fn scheduler_error_response(e: SchedulerError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: e.to_string(),
        }),
    )
        .into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;
    use zart::{DurableApi, error::SchedulerError};
    use zart::{ExecutionRecord, ExecutionStats, ListExecutionsParams};
    use zart_scheduler::ScheduleResult;

    struct NullApi;

    #[async_trait]
    impl DurableApi for NullApi {
        async fn start(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<ScheduleResult, SchedulerError> {
            unimplemented!()
        }
        async fn cancel(&self, _: &str) -> Result<bool, SchedulerError> {
            unimplemented!()
        }
        async fn status(&self, _: &str) -> Result<ExecutionRecord, SchedulerError> {
            unimplemented!()
        }
        async fn wait(
            &self,
            _: &str,
            _: Duration,
            _: Option<Duration>,
        ) -> Result<ExecutionRecord, SchedulerError> {
            unimplemented!()
        }
        async fn offer_event(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), SchedulerError> {
            unimplemented!()
        }
        async fn list_executions(
            &self,
            _: ListExecutionsParams,
        ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
            unimplemented!()
        }
        async fn stats(&self) -> Result<ExecutionStats, SchedulerError> {
            Ok(ExecutionStats::default())
        }
    }

    fn test_app() -> axum::Router {
        let state = AppState::new(Arc::new(NullApi));
        api_router(state, "/api/v1").layer(tower_http::trace::TraceLayer::new_for_http())
    }

    #[tokio::test]
    async fn healthz_returns_200() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_returns_200() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn metrics_returns_200() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
