//! Execution model types for the new per-step task row architecture.
//!
//! Tasks in the new model carry a `metadata` JSONB column that determines
//! how the worker should dispatch them. The two primary modes are:
//!
//! - **`body`**: The handler function replays from the top, scheduling child
//!   step tasks for any steps not yet completed.
//! - **`step`**: The handler function replays to execute a specific step's
//!   lambda (identified by `target_step`), then completes transactionally.

use crate::retry::RetryConfig;
use serde::{Deserialize, Serialize};
use zart_scheduler::{StepMetaType, TaskMetadata};

/// How a task should be dispatched by the worker.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    /// The main handler body is executing. Steps are scheduled as child task rows.
    /// When the body encounters an unscheduled step it inserts a child row and exits
    /// via `Err(StepError::Scheduled)`. The body task itself is then marked completed.
    Body,

    /// A specific step task is executing. The handler body replays from the top;
    /// when it reaches the step matching `target_step`, the lambda is executed.
    /// On success the step is atomically completed and a new body task scheduled.
    Step {
        /// The `step_name` this task represents — matched against `ctx.execute_step(...)` calls.
        target_step: String,
        /// What kind of step this is (controls completion behaviour).
        step_type: StepKind,
        /// How many times this step task has been retried (0 = first attempt).
        retry_attempt: usize,
        /// Retry policy for this step (None means no retries).
        retry_config: Option<RetryConfig>,
    },
}

/// Distinguishes the behaviour of a step task at completion time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// A user-defined step with a lambda.
    Step,
    /// A timed pause. No lambda — the task fires when `execution_time` arrives.
    Sleep,
    /// An event wait. Parked until `reschedule_with_event` fires.
    WaitForEvent,
}

impl ExecutionMode {
    /// Parse an `ExecutionMode` from a typed [`TaskMetadata`].
    pub fn from_task_metadata(meta: &TaskMetadata) -> Self {
        match meta {
            TaskMetadata::Body { .. } => ExecutionMode::Body,
            TaskMetadata::Step {
                step_type,
                step_name,
                retry_attempt,
                retry_config,
                ..
            } => {
                let kind = match step_type {
                    StepMetaType::Sleep => StepKind::Sleep,
                    StepMetaType::WaitForEvent => StepKind::WaitForEvent,
                    StepMetaType::Step => StepKind::Step,
                };
                let retry_config = retry_config
                    .as_ref()
                    .and_then(|v| serde_json::from_value(v.clone()).ok());
                ExecutionMode::Step {
                    target_step: step_name.clone(),
                    step_type: kind,
                    retry_attempt: *retry_attempt as usize,
                    retry_config,
                }
            }
        }
    }

    /// Parse an `ExecutionMode` from the task's raw `metadata` JSON.
    ///
    /// Thin wrapper around [`from_task_metadata`] for call sites that receive
    /// a `serde_json::Value`. Returns `ExecutionMode::Body` if the value does
    /// not conform to the [`TaskMetadata`] schema.
    pub fn from_metadata(metadata: &serde_json::Value) -> Self {
        TaskMetadata::from_json_value(metadata.clone())
            .map(|m| Self::from_task_metadata(&m))
            .unwrap_or(ExecutionMode::Body)
    }
}

pub fn is_wait_all_child(metadata: &serde_json::Value) -> bool {
    metadata
        .get("is_wait_all_child")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use zart_scheduler::{StepMetaType, TaskMetadata};

    // ── Typed API (from_task_metadata) ────────────────────────────────────────

    #[test]
    fn from_task_metadata_body() {
        let meta = TaskMetadata::body("exec-1:run:0", "exec-1");
        assert_eq!(
            ExecutionMode::from_task_metadata(&meta),
            ExecutionMode::Body
        );
    }

    #[test]
    fn from_task_metadata_step_parses_name_and_retry_attempt() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "charge-card".into(),
            retry_attempt: 1,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        let mode = ExecutionMode::from_task_metadata(&meta);
        assert!(matches!(
            mode,
            ExecutionMode::Step {
                ref target_step,
                step_type: StepKind::Step,
                retry_attempt: 1,
                retry_config: None,
            } if target_step == "charge-card"
        ));
    }

    #[test]
    fn from_task_metadata_step_defaults_retry_to_zero() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "send-email".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        let mode = ExecutionMode::from_task_metadata(&meta);
        assert!(matches!(
            mode,
            ExecutionMode::Step {
                retry_attempt: 0,
                ..
            }
        ));
    }

    #[test]
    fn from_task_metadata_step_type_sleep() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Sleep,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "__sleep".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        assert!(matches!(
            ExecutionMode::from_task_metadata(&meta),
            ExecutionMode::Step {
                step_type: StepKind::Sleep,
                ..
            }
        ));
    }

    // ── from_metadata wrapper ─────────────────────────────────────────────────

    #[test]
    fn from_metadata_body_empty_returns_body() {
        // Non-durable tasks have {} metadata — should default to Body.
        assert_eq!(
            ExecutionMode::from_metadata(&json!({})),
            ExecutionMode::Body
        );
    }

    #[test]
    fn is_wait_all_child_true_when_flag_present() {
        assert!(is_wait_all_child(&json!({ "is_wait_all_child": true })));
    }

    #[test]
    fn is_wait_all_child_false_when_absent() {
        assert!(!is_wait_all_child(&json!({})));
    }
}
