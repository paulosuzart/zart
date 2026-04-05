//! Execution model types for the new per-step task row architecture.
//!
//! Tasks in the new model carry a `metadata` JSONB column that determines
//! how the worker should dispatch them. The two primary modes are:
//!
//! - **`body`**: The handler function replays from the top, scheduling child
//!   step tasks for any steps not yet completed.
//! - **`step`**: The handler function replays to execute a specific step's
//!   lambda (identified by `target_step`), then completes transactionally.

use serde::{Deserialize, Serialize};

/// How a task should be dispatched by the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    /// The main handler body is executing. Steps are scheduled as child task rows.
    /// When the body encounters an unscheduled step it inserts a child row and exits
    /// via `Err(StepError::Scheduled)`. The body task itself is then marked completed.
    Body {
        /// Monotonically increasing counter. `0` = first run, `1` = resumed after first step, …
        segment: usize,
    },

    /// A specific step task is executing. The handler body replays from the top;
    /// when it reaches the step matching `target_step`, the lambda is executed.
    /// On success the step is atomically completed and the next body segment scheduled.
    Step {
        /// The `step_name` this task represents — matched against `ctx.step(name, ...)` calls.
        target_step: String,
        /// What kind of step this is (controls completion behaviour).
        step_type: StepKind,
        /// Body segment number to schedule on successful completion.
        /// Not meaningful for `wait_all` children (coordinator handles that).
        next_body_segment: usize,
        /// How many times this step task has been retried (0 = first attempt).
        retry_attempt: usize,
    },

    /// A coordinator task for `wait_all`. Polls child step tasks; when all complete
    /// it schedules the next body segment. No handler replay is needed.
    Coordinator {
        /// IDs of the child step tasks to wait for.
        wait_for: Vec<String>,
        /// Body segment to schedule when all children are done.
        next_segment: usize,
    },
}

/// Distinguishes the behaviour of a step task at completion time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// A user-defined step with a lambda. On completion, schedules the next body segment
    /// (unless `is_wait_all_child` is set, in which case the coordinator does it).
    Step,
    /// A timed pause. No lambda — the task fires when `execution_time` arrives.
    Sleep,
    /// A coordinator that polls wait_all children.
    WaitAll,
    /// An event wait. Parked until `reschedule_with_event` fires.
    WaitForEvent,
}

impl ExecutionMode {
    /// Parse an `ExecutionMode` from the task's `metadata` JSON.
    ///
    /// Returns `ExecutionMode::Body { segment: 0 }` if the metadata is empty
    /// or the `mode` key is absent.
    pub fn from_metadata(metadata: &serde_json::Value) -> Self {
        let mode = metadata.get("mode").and_then(|v| v.as_str()).unwrap_or("");

        match mode {
            "body" => {
                let segment = metadata
                    .get("segment")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as usize;
                ExecutionMode::Body { segment }
            }

            "step" => {
                let step_type_str = metadata
                    .get("step_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("step");

                match step_type_str {
                    "wait_all" => {
                        let next_segment = metadata
                            .get("segment")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as usize;
                        let wait_for = metadata
                            .get("wait_for")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        ExecutionMode::Coordinator { wait_for, next_segment }
                    }

                    _ => {
                        let target_step = metadata
                            .get("step_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let next_body_segment = metadata
                            .get("segment")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(1) as usize;
                        let retry_attempt = metadata
                            .get("retry_attempt")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as usize;
                        let is_wait_all_child = metadata
                            .get("is_wait_all_child")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        let step_type = if is_wait_all_child {
                            // Reuse Step kind — `is_wait_all_child` flag distinguishes
                            // completion behaviour in the context.
                            StepKind::Step
                        } else {
                            match step_type_str {
                                "sleep" => StepKind::Sleep,
                                "wait_for_event" => StepKind::WaitForEvent,
                                _ => StepKind::Step,
                            }
                        };

                        ExecutionMode::Step {
                            target_step,
                            step_type,
                            next_body_segment,
                            retry_attempt,
                        }
                    }
                }
            }

            _ => ExecutionMode::Body { segment: 0 },
        }
    }
}

/// Returns `true` if the step task is a wait_all child (coordinator handles body scheduling).
pub fn is_wait_all_child(metadata: &serde_json::Value) -> bool {
    metadata
        .get("is_wait_all_child")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Extract the coordinator task ID for a wait_all child step.
pub fn coordinator_id(metadata: &serde_json::Value) -> Option<String> {
    metadata
        .get("coordinator_id")
        .and_then(|v| v.as_str())
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn from_metadata_body_parses_segment() {
        let meta = json!({ "mode": "body", "segment": 3 });
        assert_eq!(ExecutionMode::from_metadata(&meta), ExecutionMode::Body { segment: 3 });
    }

    #[test]
    fn from_metadata_body_defaults_segment_to_zero_when_absent() {
        let meta = json!({ "mode": "body" });
        assert_eq!(ExecutionMode::from_metadata(&meta), ExecutionMode::Body { segment: 0 });
    }

    #[test]
    fn from_metadata_step_parses_name_segment_and_retry_attempt() {
        let meta = json!({
            "mode": "step",
            "step_type": "step",
            "step_name": "charge-card",
            "segment": 2,
            "retry_attempt": 1,
        });
        assert_eq!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Step {
                target_step: "charge-card".to_string(),
                step_type: StepKind::Step,
                next_body_segment: 2,
                retry_attempt: 1,
            }
        );
    }

    #[test]
    fn from_metadata_step_defaults_segment_and_retry_to_sensible_values() {
        let meta = json!({ "mode": "step", "step_name": "send-email" });
        let mode = ExecutionMode::from_metadata(&meta);
        assert!(matches!(mode, ExecutionMode::Step { next_body_segment: 1, retry_attempt: 0, .. }));
    }

    #[test]
    fn from_metadata_step_type_sleep() {
        let meta = json!({
            "mode": "step",
            "step_type": "sleep",
            "step_name": "__sleep",
            "segment": 1,
        });
        assert!(matches!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Step { step_type: StepKind::Sleep, .. }
        ));
    }

    #[test]
    fn from_metadata_wait_all_step_type_becomes_coordinator() {
        let meta = json!({
            "mode": "step",
            "step_type": "wait_all",
            "segment": 4,
            "wait_for": ["exec-1:step:a", "exec-1:step:b"],
        });
        assert_eq!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Coordinator {
                next_segment: 4,
                wait_for: vec!["exec-1:step:a".to_string(), "exec-1:step:b".to_string()],
            }
        );
    }

    #[test]
    fn from_metadata_coordinator_with_empty_wait_for() {
        let meta = json!({ "mode": "step", "step_type": "wait_all", "segment": 1 });
        assert_eq!(
            ExecutionMode::from_metadata(&meta),
            ExecutionMode::Coordinator { next_segment: 1, wait_for: vec![] }
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

    #[test]
    fn coordinator_id_extracts_value() {
        let meta = json!({ "coordinator_id": "exec-1:coord:wait_all:2" });
        assert_eq!(
            coordinator_id(&meta),
            Some("exec-1:coord:wait_all:2".to_string())
        );
    }

    #[test]
    fn coordinator_id_none_when_absent() {
        assert!(coordinator_id(&json!({})).is_none());
    }
}
