//! Admin route definitions and handler implementations.
//!
//! The admin router provides operational endpoints for managing durable
//! executions: retrying dead steps, restarting executions, selective reruns,
//! pause/resume, and run history inspection.
//!
//! Unlike the main API router (which uses `Arc<dyn DurableApi>`), the admin
//! router requires a concrete `Arc<DurableScheduler>`. This is because admin
//! operations use concrete return types that would break object-safety on the
//! trait.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use std::sync::Arc;
use zart::{DurableScheduler, admin::PauseScope, admin::RerunSpec};

use crate::{
    models::{
        ErrorResponse, ExecutionDetailResponse, ExecutionResponse, PauseRequest, PauseRuleResponse,
        PotentiallyStaleDepResponse, RerunRequest, RerunResponse, RestartRequest, RestartResponse,
        RetryStepRequest, RetryStepResponse, RunRecordResponse, StepAttemptResponse,
        StepDetailResponse,
    },
    state::AdminState,
};

/// Construct the versioned admin API router.
///
/// Requires a concrete `DurableScheduler` because admin operations use
/// concrete return types (not object-safe trait methods).
pub fn admin_router(scheduler: Arc<DurableScheduler>) -> Router {
    Router::new()
        .route(
            "/zart/admin/v1/executions/{execution_id}/retry-step",
            post(retry_step),
        )
        .route(
            "/zart/admin/v1/executions/{execution_id}/restart",
            post(restart),
        )
        .route(
            "/zart/admin/v1/executions/{execution_id}/rerun",
            post(rerun),
        )
        .route(
            "/zart/admin/v1/executions/{execution_id}/runs",
            get(list_runs),
        )
        .route(
            "/zart/admin/v1/executions/{execution_id}/detail",
            get(execution_detail),
        )
        .route("/zart/admin/v1/pause", post(create_pause))
        .route("/zart/admin/v1/pause", get(list_pauses))
        .route("/zart/admin/v1/pause/{rule_id}", post(resume_rule))
        .route("/zart/admin/v1/pause/{rule_id}", delete(delete_pause_rule))
        .with_state(AdminState { scheduler })
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `POST /zart/admin/v1/executions/:id/retry-step` — retry a dead step.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/zart/admin/v1/executions/{execution_id}/retry-step",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    request_body = RetryStepRequest,
    responses(
        (status = 200, description = "Step retried",   body = RetryStepResponse),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-executions"
))]
async fn retry_step(
    State(state): State<AdminState>,
    Path(execution_id): Path<String>,
    Json(req): Json<RetryStepRequest>,
) -> Response {
    let run_id = match get_current_run_id(&state.scheduler, &execution_id).await {
        Some(id) => id,
        None => return not_found(&execution_id),
    };

    match state
        .scheduler
        .retry_step(&run_id, &req.step_name, req.triggered_by.as_deref())
        .await
    {
        Ok(new_task_id) => {
            (StatusCode::OK, Json(RetryStepResponse { new_task_id })).into_response()
        }
        Err(zart::error::SchedulerError::Database(_))
        | Err(zart::error::SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /zart/admin/v1/executions/:id/restart` — restart entire execution.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/zart/admin/v1/executions/{execution_id}/restart",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    request_body = RestartRequest,
    responses(
        (status = 200, description = "Restarted",      body = RestartResponse),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-executions"
))]
async fn restart(
    State(state): State<AdminState>,
    Path(execution_id): Path<String>,
    Json(req): Json<RestartRequest>,
) -> Response {
    match state
        .scheduler
        .restart(&execution_id, req.payload, req.triggered_by.as_deref())
        .await
    {
        Ok(new_run_id) => (StatusCode::OK, Json(RestartResponse { new_run_id })).into_response(),
        Err(zart::error::SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /zart/admin/v1/executions/:id/rerun` — selective rerun of steps.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/zart/admin/v1/executions/{execution_id}/rerun",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    request_body = RerunRequest,
    responses(
        (status = 200, description = "Rerun started",  body = RerunResponse),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-executions"
))]
async fn rerun(
    State(state): State<AdminState>,
    Path(execution_id): Path<String>,
    Json(req): Json<RerunRequest>,
) -> Response {
    let spec = RerunSpec {
        force_rerun: req.rerun_steps,
        preserve: req.preserve_steps,
        triggered_by: req.triggered_by,
    };

    match state.scheduler.rerun_steps(&execution_id, spec).await {
        Ok(result) => {
            let body = RerunResponse {
                new_run_number: result.new_run_number,
                effective_rerun: result.effective_rerun,
                potentially_stale: result
                    .potentially_stale
                    .into_iter()
                    .map(|d| PotentiallyStaleDepResponse {
                        preserved_step: d.preserved_step,
                        possibly_depends_on: d.possibly_depends_on,
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(zart::error::SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `GET /zart/admin/v1/executions/:id/runs` — list all runs for an execution.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/zart/admin/v1/executions/{execution_id}/runs",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
    ),
    responses(
        (status = 200, description = "Run list",       body = Vec<RunRecordResponse>),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-executions"
))]
async fn list_runs(State(state): State<AdminState>, Path(execution_id): Path<String>) -> Response {
    match state.scheduler.list_runs(&execution_id).await {
        Ok(runs) => {
            let body: Vec<RunRecordResponse> = runs
                .into_iter()
                .map(|r| RunRecordResponse {
                    run_id: r.run_id,
                    execution_id: r.execution_id,
                    run_index: r.run_index,
                    payload: r.payload,
                    status: r.status.to_string(),
                    result: r.result,
                    started_at: r.started_at,
                    completed_at: r.completed_at,
                    trigger: format!("{:?}", r.trigger).to_lowercase(),
                })
                .collect();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(zart::error::SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /zart/admin/v1/pause` — create a pause rule.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/zart/admin/v1/pause",
    request_body = PauseRequest,
    responses(
        (status = 201, description = "Pause rule created",          body = PauseRuleResponse),
        (status = 422, description = "Unprocessable entity",        body = ErrorResponse),
        (status = 500, description = "Internal error",              body = ErrorResponse),
    ),
    tag = "admin-pause"
))]
async fn create_pause(State(state): State<AdminState>, Json(req): Json<PauseRequest>) -> Response {
    if let Some(ref expires_at) = req.expires_at
        && *expires_at <= chrono::Utc::now()
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error: "expiresAt must be in the future".into(),
            }),
        )
            .into_response();
    }

    let scope = PauseScope {
        execution_id: req.execution_id,
        task_name: req.task_name,
        step_pattern: req.step_pattern,
        expires_at: req.expires_at,
        triggered_by: req.triggered_by,
        reason: req.reason,
    };

    match state.scheduler.pause(scope).await {
        Ok(rule) => {
            let body = PauseRuleResponse {
                rule_id: rule.rule_id,
                execution_id: rule.scope.execution_id,
                task_name: rule.scope.task_name,
                step_pattern: rule.scope.step_pattern,
                created_at: rule.created_at,
                expires_at: rule.scope.expires_at,
                created_by: rule.scope.triggered_by,
                deleted_at: rule.deleted_at,
                reason: rule.reason,
            };
            (StatusCode::CREATED, Json(body)).into_response()
        }
        Err(e) => scheduler_error_response(e),
    }
}

/// `GET /zart/admin/v1/pause` — list pause rules.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/zart/admin/v1/pause",
    responses(
        (status = 200, description = "Pause rules",    body = Vec<PauseRuleResponse>),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-pause"
))]
async fn list_pauses(State(state): State<AdminState>) -> Response {
    match state.scheduler.list_pause_rules(None).await {
        Ok(rules) => {
            let body: Vec<PauseRuleResponse> = rules
                .into_iter()
                .map(|r| PauseRuleResponse {
                    rule_id: r.rule_id,
                    execution_id: r.scope.execution_id,
                    task_name: r.scope.task_name,
                    step_pattern: r.scope.step_pattern,
                    created_at: r.created_at,
                    expires_at: r.scope.expires_at,
                    created_by: r.scope.triggered_by,
                    deleted_at: r.deleted_at,
                    reason: r.reason,
                })
                .collect();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => scheduler_error_response(e),
    }
}

/// `POST /zart/admin/v1/pause/:rule_id` — soft-delete a pause rule (resume).
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/zart/admin/v1/pause/{rule_id}",
    params(
        ("rule_id" = String, Path, description = "Pause rule identifier"),
    ),
    responses(
        (status = 204, description = "Rule resumed"),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-pause"
))]
async fn resume_rule(State(state): State<AdminState>, Path(rule_id): Path<String>) -> Response {
    match state.scheduler.resume_rule_by_id(&rule_id, None).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("pause rule '{rule_id}' not found"),
            }),
        )
            .into_response(),
        Err(e) => scheduler_error_response(e),
    }
}

/// `DELETE /zart/admin/v1/pause/:rule_id` — semantically correct DELETE for pause rules.
#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/zart/admin/v1/pause/{rule_id}",
    params(
        ("rule_id" = String, Path, description = "Pause rule identifier"),
    ),
    responses(
        (status = 204, description = "Rule deleted"),
        (status = 404, description = "Not found",      body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tag = "admin-pause"
))]
async fn delete_pause_rule(
    State(state): State<AdminState>,
    Path(rule_id): Path<String>,
) -> Response {
    match state.scheduler.resume_rule_by_id(&rule_id, None).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("pause rule '{rule_id}' not found"),
            }),
        )
            .into_response(),
        Err(e) => scheduler_error_response(e),
    }
}

/// Query parameters for the detail endpoint.
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams, utoipa::ToSchema))]
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DetailQuery {
    /// Load steps from this specific run instead of the current run.
    run_id: Option<String>,
}

/// `GET /zart/admin/v1/executions/:id/detail` — full execution detail with steps and attempts.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/zart/admin/v1/executions/{execution_id}/detail",
    params(
        ("execution_id" = String, Path, description = "Execution identifier"),
        DetailQuery,
    ),
    responses(
        (status = 200, description = "Execution detail", body = ExecutionDetailResponse),
        (status = 404, description = "Not found",        body = ErrorResponse),
        (status = 500, description = "Internal error",   body = ErrorResponse),
    ),
    tag = "admin-executions"
))]
async fn execution_detail(
    State(state): State<AdminState>,
    Path(execution_id): Path<String>,
    Query(query): Query<DetailQuery>,
) -> Response {
    match state
        .scheduler
        .execution_detail(&execution_id, query.run_id.as_deref())
        .await
    {
        Ok(detail) => {
            let execution: ExecutionResponse = detail.execution.into();
            let runs: Vec<RunRecordResponse> = detail
                .runs
                .into_iter()
                .map(|r| RunRecordResponse {
                    run_id: r.run_id,
                    execution_id: r.execution_id,
                    run_index: r.run_index,
                    payload: r.payload,
                    status: r.status.to_string(),
                    result: r.result,
                    started_at: r.started_at,
                    completed_at: r.completed_at,
                    trigger: format!("{:?}", r.trigger).to_lowercase(),
                })
                .collect();
            let steps: Vec<StepDetailResponse> = detail
                .steps
                .into_iter()
                .map(|s| {
                    let attempts: Vec<StepAttemptResponse> = s
                        .attempts
                        .into_iter()
                        .map(|a| StepAttemptResponse {
                            attempt_number: a.attempt_number,
                            status: format!("{:?}", a.status).to_lowercase(),
                            result: a.result,
                            error: a.error,
                            started_at: a.started_at,
                            completed_at: a.completed_at,
                        })
                        .collect();
                    StepDetailResponse {
                        step_id: s.step.step_id,
                        name: s.step.step_name,
                        kind: format!("{:?}", s.step.step_kind).to_lowercase(),
                        status: format!("{:?}", s.step.status).to_lowercase(),
                        retry_attempt: s.step.retry_attempt,
                        result: s.step.result,
                        last_error: s.step.last_error,
                        retryable: s.retryable,
                        scheduled_at: s.step.scheduled_at,
                        completed_at: s.step.completed_at,
                        attempts,
                    }
                })
                .collect();
            let body = ExecutionDetailResponse {
                execution,
                runs,
                steps,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(zart::error::SchedulerError::ExecutionNotFound(_)) => not_found(&execution_id),
        Err(e) => scheduler_error_response(e),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Get the current run_id for an execution.
async fn get_current_run_id(scheduler: &DurableScheduler, execution_id: &str) -> Option<String> {
    scheduler.get_current_run_id(execution_id).await.ok()?
}

fn not_found(execution_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("execution '{execution_id}' not found"),
        }),
    )
        .into_response()
}

fn scheduler_error_response(e: zart::error::SchedulerError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: e.to_string(),
        }),
    )
        .into_response()
}
