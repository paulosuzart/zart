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
    /// Legacy mode — no `metadata` present. Uses the old JSON-blob-in-state approach.
    /// Preserved for backward compatibility; existing handlers continue to work unchanged.
    Legacy,

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
    /// Returns `ExecutionMode::Legacy` if the metadata is empty or the `mode`
    /// key is absent, preserving full backward compatibility.
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

            _ => ExecutionMode::Legacy,
        }
    }

    /// Returns `true` if this is the new execution model (non-legacy).
    pub fn is_new_model(&self) -> bool {
        !matches!(self, ExecutionMode::Legacy)
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
