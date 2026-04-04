//! Route definitions for the Zart HTTP API.
//!
//! All handler functions are stubs that will be filled in during M5.

use axum::{
    Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};

/// Construct the versioned API router.
pub fn api_router() -> Router {
    Router::new()
        // Execution management
        .route("/api/v1/executions", get(list_executions))
        .route("/api/v1/executions", post(start_execution))
        .route("/api/v1/executions/{execution_id}", get(get_execution))
        .route(
            "/api/v1/executions/{execution_id}/cancel",
            post(cancel_execution),
        )
        .route(
            "/api/v1/executions/{execution_id}/wait",
            get(wait_execution),
        )
        // Event delivery
        .route(
            "/api/v1/events/{execution_id}/{event_name}",
            post(offer_event),
        )
        // Health check
        .route("/healthz", get(healthz))
}

/// `GET /healthz` — liveness probe.
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `GET /api/v1/executions` — list executions with optional filters.
async fn list_executions() -> impl IntoResponse {
    // TODO(M5): query executions from the database with pagination.
    StatusCode::NOT_IMPLEMENTED
}

/// `POST /api/v1/executions` — start a new durable execution.
async fn start_execution() -> impl IntoResponse {
    // TODO(M5): deserialize body, call DurableScheduler::start.
    StatusCode::NOT_IMPLEMENTED
}

/// `GET /api/v1/executions/:execution_id` — get execution status and step progress.
async fn get_execution() -> impl IntoResponse {
    // TODO(M5): fetch execution record and serialise as JSON.
    StatusCode::NOT_IMPLEMENTED
}

/// `POST /api/v1/executions/:execution_id/cancel` — cancel a running execution.
async fn cancel_execution() -> impl IntoResponse {
    // TODO(M5): call DurableScheduler::cancel.
    StatusCode::NOT_IMPLEMENTED
}

/// `GET /api/v1/executions/:execution_id/wait` — long-poll until completion.
async fn wait_execution() -> impl IntoResponse {
    // TODO(M5): poll with exponential backoff and return when terminal.
    StatusCode::NOT_IMPLEMENTED
}

/// `POST /api/v1/events/:execution_id/:event_name` — deliver an event.
async fn offer_event() -> impl IntoResponse {
    // TODO(M5): call DurableScheduler::offer_event.
    StatusCode::NOT_IMPLEMENTED
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn healthz_returns_200() {
        let app = api_router();
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
}
