//! Declarative step type core definitions and behavior interfaces (v3 plan scaffold).
//!
//! This module intentionally provides **only** the core contracts and type routing.
//! Implementations and wiring live in sibling modules and later phases.

use async_trait::async_trait;
use scheduler::{StorageBackend, StorageError};

use crate::context::{PendingFn, TaskContext};
use crate::error::StepError;

/// Result produced by step-mode behavior.
///
/// `Transition` is used for step kinds that do not yield a value (e.g. sleep).
#[derive(Debug, Clone)]
pub enum StepResult {
    /// Lambda was executed and produced a serialized value.
    Executed(serde_json::Value),
    /// Value came from an already-completed cached step.
    Cached(serde_json::Value),
    /// No value payload; completion layer performs transition logic.
    Transition,
}

/// Distinguishes success/failure completion routing.
///
/// Used by completion implementations that need to branch behavior.
#[derive(Debug, Clone)]
pub enum CompletionOutcome {
    Success,
    Failure { error: String },
}

/// Wait-group semantics used by TaskContext wait APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitGroupKind {
    All,
    Any,
    FirstN(usize),
}

impl WaitGroupKind {
    /// Converts wait-group kind into a trigger threshold for `wg_remaining`.
    ///
    /// - all: trigger when remaining == 0
    /// - any: trigger when remaining == total - 1
    /// - first_n: trigger when remaining == total - n
    pub fn trigger_threshold(self, total: usize) -> Result<i32, StepError> {
        let total_i32 = i32::try_from(total).map_err(|_| StepError::Failed {
            step: "__wait_group".to_string(),
            reason: "too many wait-group handles to fit i32 threshold".to_string(),
        })?;

        match self {
            Self::All => Ok(0),
            Self::Any => {
                if total == 0 {
                    return Err(StepError::Failed {
                        step: "__wait_any".to_string(),
                        reason: "wait_any requires at least one handle".to_string(),
                    });
                }
                Ok(total_i32 - 1)
            }
            Self::FirstN(n) => {
                if n == 0 {
                    return Err(StepError::Failed {
                        step: "__wait_first_n".to_string(),
                        reason: "wait_first_n requires n > 0".to_string(),
                    });
                }
                if n > total {
                    return Err(StepError::Failed {
                        step: "__wait_first_n".to_string(),
                        reason: format!(
                            "wait_first_n requires n <= handles.len() (n={n}, len={total})"
                        ),
                    });
                }
                let n_i32 = i32::try_from(n).map_err(|_| StepError::Failed {
                    step: "__wait_first_n".to_string(),
                    reason: "n does not fit i32".to_string(),
                })?;
                Ok(total_i32 - n_i32)
            }
        }
    }
}

/// Payload provided to completion behaviors.
#[derive(Debug, Clone)]
pub struct CompletionSpec {
    pub step_task_id: String,
    pub step_id: String,
    pub step_name: String,
    pub worker_id: String,
    pub task_name: String,
    pub run_id: String,
    pub execution_id: String,
    pub data: serde_json::Value,
    pub attempt_number: usize,
    pub result: StepResult,

    /// Optional wait-group parent step name.
    pub wait_group_step_name: Option<String>,

    /// Success/failure routing info.
    pub outcome: CompletionOutcome,
}

/// Body-mode behavior contract.
#[async_trait]
pub trait BodyBehavior: Send + Sync {
    /// Called when body mode encounters this step type.
    ///
    /// Returns cached result or `Err(StepError::Scheduled)` when step is in-flight
    /// or newly scheduled.
    async fn handle(
        &self,
        ctx: &mut TaskContext,
        step_name: &str,
    ) -> Result<serde_json::Value, StepError>;
}

/// Step-mode behavior contract.
#[async_trait]
pub trait StepBehavior: Send + Sync {
    /// Called when step mode targets this step type.
    ///
    /// Must not execute completion logic directly.
    async fn handle(
        &self,
        ctx: &mut TaskContext,
        step_name: &str,
        lambda: Option<PendingFn>,
    ) -> Result<StepResult, StepError>;
}

/// Completion behavior contract.
#[async_trait]
pub trait CompletionBehavior: Send + Sync {
    /// Called after step behavior returns `Ok`.
    ///
    /// Must atomically complete step task and schedule next work when needed.
    async fn complete(
        &self,
        scheduler: &dyn StorageBackend,
        spec: CompletionSpec,
    ) -> Result<(), StorageError>;
}

/// Thin step-definition routing enum.
///
/// This resolves metadata into a concrete set of behavior traits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepDefId {
    Step,
    Sleep,
    WaitForEvent,
    WaitGroupChild,
}

impl StepDefId {
    /// Resolve step definition id from task metadata.
    ///
    /// Backward-compat:
    /// - accepts new `wg_step_name`
    /// - accepts old `is_wait_all_child`
    pub fn from_metadata(metadata: &serde_json::Value) -> Self {
        let is_wait_group_child = metadata
            .get("wg_step_name")
            .and_then(|v| v.as_str())
            .is_some()
            || metadata
                .get("is_wait_all_child")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

        if is_wait_group_child {
            return Self::WaitGroupChild;
        }

        match metadata
            .get("step_type")
            .and_then(|v| v.as_str())
            .unwrap_or("step")
        {
            "sleep" => Self::Sleep,
            "wait_for_event" => Self::WaitForEvent,
            _ => Self::Step,
        }
    }

    pub fn body_behavior(self) -> &'static dyn BodyBehavior {
        match self {
            Self::Step => &body::LookupOrSchedule,
            Self::Sleep => &body::LookupOrSchedule,
            Self::WaitForEvent => &body::LookupOrScheduleEvent,
            Self::WaitGroupChild => &body::LookupOrSchedule,
        }
    }

    pub fn step_behavior(self) -> &'static dyn StepBehavior {
        match self {
            Self::Step => &step::ExecuteLambda,
            Self::Sleep => &step::TransitionOnly,
            Self::WaitForEvent => &step::LookupCached,
            Self::WaitGroupChild => &step::ExecuteLambda,
        }
    }

    pub fn completion_behavior(self) -> &'static dyn CompletionBehavior {
        match self {
            Self::Step => &completion::ScheduleNextBody,
            Self::Sleep => &completion::ScheduleNextBody,
            Self::WaitForEvent => &completion::FailExecutionOnDeadline,
            Self::WaitGroupChild => &completion::DecrementAndMaybeResume,
        }
    }
}

pub mod body;
pub mod completion;
pub mod dispatch;
pub mod step;
