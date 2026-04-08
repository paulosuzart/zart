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
    /// Parse an `ExecutionMode` from the task's `metadata` JSON.
    ///
    /// Returns `ExecutionMode::Body` if the metadata is empty
    /// or the `mode` key is absent.
    pub fn from_metadata(metadata: &serde_json::Value) -> Self {
        let mode = metadata.get("mode").and_then(|v| v.as_str()).unwrap_or("");

        match mode {
            "body" => ExecutionMode::Body,

            "step" => {
                let step_type_str = metadata
                    .get("step_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("step");

                let target_step = metadata
                    .get("step_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let retry_attempt = metadata
                    .get("retry_attempt")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as usize;
                let step_type = match step_type_str {
                    "sleep" => StepKind::Sleep,
                    "wait_for_event" => StepKind::WaitForEvent,
                    _ => StepKind::Step,
                };

                let retry_config = metadata
                    .get("retry_config")
                    .and_then(|v| serde_json::from_value(v.clone()).ok());

                ExecutionMode::Step {
                    target_step,
                    step_type,
                    retry_attempt,
                    retry_config,
                }
            }

            _ => ExecutionMode::Body,
        }
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

    #[test]
    fn from_metadata_body() {
        let meta = json!({ "mode": "body" });
        assert_eq!(ExecutionMode::from_metadata(&meta), ExecutionMode::Body);
    }

    #[test]
    fn from_metadata_body_empty() {
        let meta = json!({});
        assert_eq!(ExecutionMode::from_metadata(&meta), ExecutionMode::Body);
    }

    #[test]
    fn from_metadata_step_parses_name_and_retry_attempt() {
        let meta = json!({
            "mode": "step",
            "step_type": "step",
            "step_name": "charge-card",
            "retry_attempt": 1,
        });
        let mode = ExecutionMode::from_metadata(&meta);
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
    fn from_metadata_step_defaults_retry_to_zero() {
        let meta = json!({ "mode": "step", "step_name": "send-email" });
        let mode = ExecutionMode::from_metadata(&meta);
        assert!(matches!(
            mode,
            ExecutionMode::Step {
                retry_attempt: 0,
                ..
            }
        ));
    }

    #[test]
    fn from_metadata_step_type_sleep() {
        let meta = json!({
            "mode": "step",
            "step_type": "sleep",
            "step_name": "__sleep",
        });
        assert!(matches!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Step {
                step_type: StepKind::Sleep,
                ..
            }
        ));
    }

    #[test]
    fn from_metadata_wait_all_step_type_becomes_step_kind() {
        let meta = json!({
            "mode": "step",
            "step_type": "wait_all",
            "wait_for": ["exec-1:step:a", "exec-1:step:b"],
        });
        assert!(matches!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Step {
                target_step,
                step_type: StepKind::Step,
                retry_attempt: 0,
                retry_config: None,
            } if target_step.is_empty()
        ));
    }

    #[test]
    fn from_metadata_wait_all_without_wait_for_is_still_step_kind() {
        let meta = json!({ "mode": "step", "step_type": "wait_all" });
        assert!(matches!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Step {
                target_step,
                step_type: StepKind::Step,
                retry_attempt: 0,
                retry_config: None,
            } if target_step.is_empty()
        ));
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
