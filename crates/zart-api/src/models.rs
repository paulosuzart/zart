//! Request and response types for the Zart HTTP API.

use chrono::{DateTime, Utc};
use scheduler::ExecutionRecord;
use serde::{Deserialize, Serialize};

// ── Requests ──────────────────────────────────────────────────────────────────

/// Body for `POST /api/v1/executions`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartExecutionRequest {
    /// Idempotency key. If an execution with this ID already exists the call
    /// is a no-op and the existing record is returned.
    pub execution_id: String,
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
    /// Filter by lifecycle status (scheduled, running, completed, failed, cancelled).
    pub status: Option<String>,
    /// Filter by registered task name.
    pub task_name: Option<String>,
    /// Maximum number of results to return (default: 20).
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Number of results to skip for pagination (default: 0).
    #[serde(default)]
    pub offset: usize,
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
