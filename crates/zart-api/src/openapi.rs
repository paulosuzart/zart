//! OpenAPI documentation for the Zart HTTP API.
//!
//! This module is only compiled when the `openapi` feature is enabled.
//! It exports [`ZartApiDoc`], a type that implements [`utoipa::OpenApi`] and
//! can be merged into an existing utoipa API doc via the `nest` attribute.
//!
//! # Example — merge into your own API doc
//!
//! ```rust,ignore
//! use zart_api::openapi::ZartApiDoc;
//!
//! #[derive(utoipa::OpenApi)]
//! #[openapi(
//!     nest(
//!         (path = "/", api = ZartApiDoc)
//!     )
//! )]
//! struct MyAppDoc;
//! ```

use crate::models::{
    ErrorResponse, ExecutionDetailResponse, ExecutionResponse, ListQuery, PauseRequest,
    PauseRuleResponse, PotentiallyStaleDepResponse, RerunRequest, RerunResponse, RestartRequest,
    RestartResponse, ResumeResponse, RetryStepRequest, RetryStepResponse, RunRecordResponse,
    StartExecutionRequest, StartExecutionResponse, StatsResponse, StepAttemptResponse,
    StepDetailResponse, WaitQuery,
};
use crate::{admin_routes, routes};

/// OpenAPI 3.x schema document for the Zart HTTP API.
///
/// Covers both the Main API (`/api/v1/*`) and the Admin API (`/admin/v1/*`).
/// Use the [`utoipa::OpenApi::nest`] pattern to incorporate this schema into
/// an existing application-level `ApiDoc`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        routes::list_executions,
        routes::start_execution,
        routes::get_execution,
        routes::cancel_execution,
        routes::wait_execution,
        routes::offer_event,
        routes::get_stats,
        routes::healthz,
        routes::readyz,
        admin_routes::retry_step,
        admin_routes::restart,
        admin_routes::rerun,
        admin_routes::execution_detail,
        admin_routes::list_runs,
        admin_routes::create_pause,
        admin_routes::list_pauses,
        admin_routes::resume_rule,
        admin_routes::delete_pause_rule,
    ),
    components(schemas(
        StartExecutionRequest,
        ListQuery,
        WaitQuery,
        ExecutionResponse,
        StartExecutionResponse,
        ErrorResponse,
        RetryStepRequest,
        RestartRequest,
        RerunRequest,
        RetryStepResponse,
        RestartResponse,
        RerunResponse,
        PotentiallyStaleDepResponse,
        RunRecordResponse,
        PauseRequest,
        PauseRuleResponse,
        ResumeResponse,
        StatsResponse,
        ExecutionDetailResponse,
        StepDetailResponse,
        StepAttemptResponse,
    )),
    tags(
        (name = "executions",       description = "Durable execution lifecycle"),
        (name = "events",           description = "External event delivery"),
        (name = "stats",            description = "Aggregate execution statistics"),
        (name = "health",           description = "Liveness and readiness probes"),
        (name = "admin-executions", description = "Administrative execution operations"),
        (name = "admin-pause",      description = "Pause rule management"),
    ),
    info(title = "Zart API", version = env!("CARGO_PKG_VERSION"))
)]
pub struct ZartApiDoc;

/// Build an Axum router that serves the Swagger UI at `/swagger-ui` and the
/// OpenAPI schema at `/openapi.json`.
///
/// Merge this into your existing router to expose the docs without pulling in
/// `utoipa` or `utoipa-swagger-ui` directly.
///
/// # Example
///
/// ```rust,ignore
/// let router = my_router().merge(zart_api::openapi::swagger_ui_router());
/// ```
pub fn swagger_ui_router() -> axum::Router {
    use utoipa::OpenApi as _;
    utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
        .url("/openapi.json", ZartApiDoc::openapi())
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi as _;

    #[test]
    fn zart_api_doc_has_paths() {
        let doc = ZartApiDoc::openapi();
        assert!(
            !doc.paths.paths.is_empty(),
            "ZartApiDoc must expose at least one path"
        );
    }

    #[test]
    fn zart_api_doc_serialises_to_json() {
        let doc = ZartApiDoc::openapi();
        let json = serde_json::to_string(&doc).expect("serialisation must succeed");
        assert!(json.contains("Zart API"), "title must appear in JSON");
    }
}
