//! Request and response types for the Zart HTTP API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use zart::{ExecutionRecord, ExecutionSortField, ListExecutionsParams, SortOrder};

// ── Requests ──────────────────────────────────────────────────────────────────

/// Body for `POST /api/v1/executions`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartExecutionRequest {
    /// Idempotency key. Generated as a UUID v4 when omitted.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Registered task handler name.
    pub task_name: String,
    /// Arbitrary JSON payload forwarded to the task handler.
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// Query parameters for `GET /api/v1/executions`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    pub status: Option<String>,
    pub task_name: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub search: Option<String>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

impl ListQuery {
    pub fn into_params(self) -> ListExecutionsParams {
        let status = self
            .status
            .as_deref()
            .and_then(|s| s.parse::<zart::ExecutionStatus>().ok());
        let sort_by = match self.sort_by.as_deref() {
            Some("status") => ExecutionSortField::Status,
            Some("taskName") => ExecutionSortField::TaskName,
            _ => ExecutionSortField::ScheduledAt,
        };
        let sort_order = match self.sort_order.as_deref() {
            Some("asc") => SortOrder::Asc,
            _ => SortOrder::Desc,
        };
        ListExecutionsParams {
            status,
            task_name: self.task_name,
            from: self.from,
            to: self.to,
            search: self.search,
            sort_by,
            sort_order,
            limit: self.limit,
            offset: self.offset,
        }
    }
}

fn default_limit() -> usize {
    20
}

/// Query parameters for `GET /api/v1/executions/:id/wait`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitQuery {
    /// Maximum seconds to wait (capped at 30, default: 30).
    pub timeout_secs: Option<u64>,
}

// ── Responses ─────────────────────────────────────────────────────────────────

/// JSON representation of a durable execution record.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionResponse {
    /// Registered task handler name.
    pub name: String,
    /// Unique execution identifier (idempotency key).
    pub durable_execution_id: String,
    /// Original JSON payload.
    pub payload: serde_json::Value,
    /// Lifecycle status (scheduled | running | completed | failed | cancelled).
    pub status: String,
    /// When the execution was first scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// When the execution reached a terminal state (`null` if still running).
    pub completed_at: Option<DateTime<Utc>>,
    /// Schema version counter.
    pub version: i32,
    /// JSON result produced by the task handler (`null` if not yet completed).
    pub result: Option<serde_json::Value>,
}

impl From<ExecutionRecord> for ExecutionResponse {
    fn from(r: ExecutionRecord) -> Self {
        Self {
            name: r.task_name,
            durable_execution_id: r.execution_id,
            payload: r.payload,
            status: r.status.to_string(),
            scheduled_at: r.scheduled_at,
            completed_at: r.completed_at,
            version: r.version,
            result: r.result,
        }
    }
}

/// Body returned for a successful start.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartExecutionResponse {
    pub execution_id: String,
    pub task_id: String,
}

/// Body for error responses.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Admin Requests ────────────────────────────────────────────────────────────

/// Body for `POST /admin/v1/executions/:id/retry-step`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryStepRequest {
    pub step_name: String,
    #[serde(default)]
    pub triggered_by: Option<String>,
}

/// Body for `POST /admin/v1/executions/:id/restart`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartRequest {
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub triggered_by: Option<String>,
}

/// Body for `POST /admin/v1/executions/:id/rerun`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RerunRequest {
    #[serde(default)]
    pub rerun_steps: Vec<String>,
    #[serde(default)]
    pub preserve_steps: Vec<String>,
    #[serde(default)]
    pub triggered_by: Option<String>,
}

// ── Admin Responses ───────────────────────────────────────────────────────────

/// Body returned for a successful retry-step.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryStepResponse {
    pub new_task_id: String,
}

/// Body returned for a successful restart.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartResponse {
    pub new_run_id: String,
}

/// Body returned for a successful rerun.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RerunResponse {
    pub new_run_number: u32,
    pub effective_rerun: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub potentially_stale: Vec<PotentiallyStaleDepResponse>,
}

/// A preserved step that may have a stale dependency on a rerun step.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PotentiallyStaleDepResponse {
    pub preserved_step: String,
    pub possibly_depends_on: Vec<String>,
}

/// A single run record returned from the runs list.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRecordResponse {
    pub run_id: String,
    pub execution_id: String,
    pub run_index: i32,
    pub payload: serde_json::Value,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub trigger: String,
}

// ── Pause / Resume Types ──────────────────────────────────────────────────────

/// Body for `POST /admin/v1/pause`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PauseRequest {
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub task_name: Option<String>,
    #[serde(default)]
    pub step_pattern: Option<String>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub triggered_by: Option<String>,
}

/// Response for a single pause rule.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PauseRuleResponse {
    pub rule_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub task_name: Option<String>,
    #[serde(default)]
    pub step_pattern: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Response for a resume operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeResponse {
    pub rules_deleted: usize,
}

/// Response for `GET /api/v1/stats`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsResponse {
    pub scheduled: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

impl From<zart::ExecutionStats> for StatsResponse {
    fn from(s: zart::ExecutionStats) -> Self {
        Self {
            scheduled: s.scheduled,
            running: s.running,
            completed: s.completed,
            failed: s.failed,
            cancelled: s.cancelled,
        }
    }
}

/// Response for `GET /admin/v1/executions/:id/detail`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDetailResponse {
    pub execution: ExecutionResponse,
    pub runs: Vec<RunRecordResponse>,
    pub steps: Vec<StepDetailResponse>,
}

/// A step with its attempt history.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepDetailResponse {
    pub step_id: String,
    pub name: String,
    pub kind: String,
    pub status: String,
    pub retry_attempt: i32,
    pub result: Option<serde_json::Value>,
    pub last_error: Option<String>,
    pub retryable: bool,
    pub scheduled_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub attempts: Vec<StepAttemptResponse>,
}

/// A single step attempt in the detail response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepAttemptResponse {
    pub attempt_number: i32,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}
