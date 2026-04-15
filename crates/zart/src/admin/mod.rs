//! Admin operation types for durable execution management.
//!
//! These types define the parameters and results for administrative operations
//! like step retry, selective rerun, full restart, and pause/resume.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Admin Operation Context ───────────────────────────────────────────────────

/// Metadata about an admin operation that triggered a re-entry.
///
/// Stored in `ExecutionState.admin_context` and surfaced to handler and step
/// code via `TaskContext::admin_context()` during the duration of the admin
/// operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminOperationContext {
    /// What admin operation was performed.
    pub operation: AdminOperation,
    /// The run number this operation applies to.
    pub run_number: u32,
    /// When the operator triggered the operation.
    pub triggered_at: DateTime<Utc>,
    /// Optional identifier of the operator (user, system, etc.).
    pub triggered_by: Option<String>,
}

/// Discriminant of admin operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminOperation {
    /// A single step was retried after reaching dead status.
    ManualStepRetry {
        /// Name of the step that was retried.
        step_name: String,
    },
    /// A selective re-run of specific steps was initiated.
    SelectiveRerun {
        /// Steps that will be re-executed in the new run.
        rerun_steps: Vec<String>,
        /// Completed steps whose results are carried forward.
        preserved_steps: Vec<String>,
    },
    /// The entire execution was restarted from scratch.
    FullRestart {
        /// Status of the execution before restart.
        previous_status: String,
    },
}

// ── Selective Rerun ───────────────────────────────────────────────────────────

/// Specification for which steps to rerun vs preserve.
///
/// Passed to [`DurableScheduler::rerun_steps`](crate::DurableScheduler::rerun_steps).
#[derive(Debug, Clone, Default)]
pub struct RerunSpec {
    /// Steps to force-rerun even if currently completed.
    pub force_rerun: Vec<String>,
    /// Completed steps to explicitly preserve (ignored for failed/dead steps —
    /// those are always rerun).
    pub preserve: Vec<String>,
    /// Optional operator identifier for audit logging.
    pub triggered_by: Option<String>,
}

/// Result of a selective rerun operation.
#[derive(Debug, Clone)]
pub struct RerunResult {
    /// The run number of the newly started run.
    pub new_run_number: u32,
    /// Steps that will be rerun (union of force_rerun + auto-failed/dead).
    pub effective_rerun: Vec<String>,
}

// ── Pause / Resume ────────────────────────────────────────────────────────────

/// Scope of a pause operation.
///
/// At least one of `execution_id` or `task_name` must be `Some`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PauseScope {
    /// Target a specific execution.
    pub execution_id: Option<String>,
    /// Target all executions of a given task name.
    pub task_name: Option<String>,
    /// Glob pattern for step names (e.g. `"send-*"`). `None` means all steps.
    pub step_pattern: Option<String>,
    /// Optional auto-expiry for the pause rule.
    pub expires_at: Option<DateTime<Utc>>,
    /// Optional operator identifier for audit logging.
    pub triggered_by: Option<String>,
}

/// A pause rule stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseRule {
    /// Unique rule identifier.
    pub rule_id: String,
    /// Scope of this rule.
    pub scope: PauseScope,
    /// When the rule was created.
    pub created_at: DateTime<Utc>,
    /// When the rule was soft-deleted (None = active).
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Result of a resume operation.
#[derive(Debug, Clone)]
pub struct ResumeResult {
    /// Number of pause rules that were soft-deleted.
    pub rules_deleted: usize,
}

/// Full execution detail for the admin detail endpoint.
#[derive(Debug, Clone)]
pub struct ExecutionDetail {
    pub execution: zart_scheduler::ExecutionRecord,
    pub runs: Vec<zart_scheduler::ExecutionRunRecord>,
    pub steps: Vec<StepWithAttempts>,
}

/// A step with its attempts and retryability flag.
#[derive(Debug, Clone)]
pub struct StepWithAttempts {
    pub step: zart_scheduler::StepRow,
    pub attempts: Vec<zart_scheduler::StepAttemptRow>,
    pub retryable: bool,
}
