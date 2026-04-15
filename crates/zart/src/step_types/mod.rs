//! Declarative step type core definitions and behavior interfaces.
//!
//! This module provides the core contracts and type routing for declarative
//! step dispatch. All orchestration lives in behavior implementations and the
//! dispatch layer; `TaskContext` only expresses intent.

use async_trait::async_trait;
use zart_scheduler::{StorageBackend, StorageError};

use crate::context::{PendingFn, TaskContext};
use crate::error::StepError;
use crate::retry::RetryConfig;

// ── StepRequest ───────────────────────────────────────────────────────────────

/// Unified invocation request flowing from `TaskContext` into dispatch.
///
/// Carries all parameters needed by body and step behaviors so that
/// `TaskContext` itself remains a thin intent-capture layer.
pub struct StepRequest<'a> {
    /// Step name identifying this invocation.
    pub step_name: &'a str,
    /// Kind-specific parameters.
    pub kind: StepRequestKind<'a>,
    /// Retry policy for this step (used by body behavior to embed in metadata).
    pub retry_config: Option<&'a RetryConfig>,
    /// Timeout duration for this step (used when timeout_scope == Global).
    /// When set, the body behavior computes a deadline and writes it to metadata.
    pub timeout: Option<std::time::Duration>,
}

/// Kind-specific parameters for a step invocation.
pub enum StepRequestKind<'a> {
    /// A regular user-defined step.
    Step,
    /// A timed pause; fires when `wake_time` arrives.
    Sleep {
        wake_time: chrono::DateTime<chrono::Utc>,
    },
    /// An event wait; parked until an external event is delivered.
    WaitForEvent {
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// A wait-group barrier: ensure child rows exist and upsert the group row.
    WaitGroupBarrier {
        group_step_name: &'a str,
        child_names: &'a [String],
        threshold: i32,
    },
    /// A wait-group child step executing its lambda.
    WaitGroupChild,
}

impl<'a> StepRequest<'a> {
    pub fn new_step(
        step_name: &'a str,
        retry_config: Option<&'a RetryConfig>,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        Self {
            step_name,
            kind: StepRequestKind::Step,
            retry_config,
            timeout,
        }
    }

    pub fn new_sleep(step_name: &'a str, wake_time: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            step_name,
            kind: StepRequestKind::Sleep { wake_time },
            retry_config: None,
            timeout: None,
        }
    }

    pub fn new_wait_for_event(
        step_name: &'a str,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        Self {
            step_name,
            kind: StepRequestKind::WaitForEvent { deadline },
            retry_config: None,
            timeout: None,
        }
    }

    pub fn new_wait_group_barrier(
        step_name: &'a str,
        child_names: &'a [String],
        threshold: i32,
    ) -> Self {
        Self {
            step_name,
            kind: StepRequestKind::WaitGroupBarrier {
                group_step_name: step_name,
                child_names,
                threshold,
            },
            retry_config: None,
            timeout: None,
        }
    }

    pub fn new_wait_group_child(step_name: &'a str) -> Self {
        Self {
            step_name,
            kind: StepRequestKind::WaitGroupChild,
            retry_config: None,
            timeout: None,
        }
    }
}

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

/// Discriminant for the kind of terminal outcome a step row holds.
///
/// Stored in `zart_steps.result_kind` and returned alongside the raw JSON
/// from body-mode lookup so the caller can construct the correct
/// [`StepOutcome`](crate::error::StepOutcome) variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultKind {
    /// Step succeeded — `result` holds `S::Output`.
    Ok,
    /// Step returned a business error — `result` holds `S::Error`.
    Err,
    /// Retries exhausted — `result` holds `S::Error` from the last attempt.
    RetryExhausted,
    /// Step timed out — `result` is NULL.
    TimedOut,
    /// wait_for_event deadline exceeded — `result` is NULL.
    DeadlineExceeded,
}

impl ResultKind {
    /// Parse from a database value (or default to `'ok'`).
    pub fn from_db(kind: Option<&str>) -> Self {
        match kind {
            Some("ok") => Self::Ok,
            Some("err") => Self::Err,
            Some("rx") => Self::RetryExhausted,
            Some("timeout") => Self::TimedOut,
            Some("dl") => Self::DeadlineExceeded,
            _ => Self::Ok, // backward compat: old rows without result_kind
        }
    }
}

impl ResultKind {
    /// The DB string value to store.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
            Self::RetryExhausted => "rx",
            Self::TimedOut => "timeout",
            Self::DeadlineExceeded => "dl",
        }
    }
}

impl From<ResultKind> for zart_scheduler::StepResultKind {
    fn from(k: ResultKind) -> Self {
        match k {
            ResultKind::Ok => Self::Ok,
            ResultKind::Err => Self::Err,
            ResultKind::RetryExhausted => Self::Rx,
            ResultKind::TimedOut => Self::Timeout,
            ResultKind::DeadlineExceeded => Self::Dl,
        }
    }
}

impl From<zart_scheduler::StepResultKind> for ResultKind {
    fn from(k: zart_scheduler::StepResultKind) -> Self {
        match k {
            zart_scheduler::StepResultKind::Ok => Self::Ok,
            zart_scheduler::StepResultKind::Err => Self::Err,
            zart_scheduler::StepResultKind::Rx => Self::RetryExhausted,
            zart_scheduler::StepResultKind::Timeout => Self::TimedOut,
            zart_scheduler::StepResultKind::Dl => Self::DeadlineExceeded,
        }
    }
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
    /// Returns `(cached_result, result_kind)` or `Err(StepError::Scheduled)`
    /// when step is in-flight or newly scheduled.
    async fn handle(
        &self,
        ctx: &TaskContext,
        req: &StepRequest<'_>,
    ) -> Result<(serde_json::Value, ResultKind), StepError>;
}

/// Step-mode behavior contract.
#[async_trait]
pub trait StepBehavior: Send + Sync {
    /// Called when step mode targets this step type.
    ///
    /// Must not execute completion logic directly.
    async fn handle(
        &self,
        ctx: &TaskContext,
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
    /// Wait-group barrier: body behavior ensures child rows exist and upserts
    /// the group row; not a step that is ever executed in step mode.
    WaitGroupBarrier,
}

impl StepDefId {
    /// Derive step definition id from a `StepRequestKind`.
    pub fn from_kind(kind: &StepRequestKind<'_>) -> Self {
        match kind {
            StepRequestKind::Step => Self::Step,
            StepRequestKind::Sleep { .. } => Self::Sleep,
            StepRequestKind::WaitForEvent { .. } => Self::WaitForEvent,
            StepRequestKind::WaitGroupBarrier { .. } => Self::WaitGroupBarrier,
            StepRequestKind::WaitGroupChild => Self::WaitGroupChild,
        }
    }

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
            Self::Sleep => &body::LookupOrScheduleSleep,
            Self::WaitForEvent => &body::LookupOrScheduleEvent,
            Self::WaitGroupChild => &body::LookupOrSchedule,
            Self::WaitGroupBarrier => &body::LookupOrScheduleWaitGroupBarrier,
        }
    }

    pub fn step_behavior(self) -> &'static dyn StepBehavior {
        match self {
            Self::Step => &step::ExecuteLambda,
            Self::Sleep => &step::TransitionOnly,
            Self::WaitForEvent => &step::LookupCached,
            Self::WaitGroupChild => &step::ExecuteLambda,
            // WaitGroupBarrier is never executed in step mode.
            Self::WaitGroupBarrier => &step::ExecuteLambda,
        }
    }

    pub fn completion_behavior(
        self,
        outcome: &CompletionOutcome,
    ) -> &'static dyn CompletionBehavior {
        match self {
            Self::Step => &completion::ScheduleNextBody,
            Self::Sleep => &completion::ScheduleNextBody,
            Self::WaitForEvent => &completion::FailExecutionOnDeadline,
            Self::WaitGroupChild => match outcome {
                CompletionOutcome::Success => &completion::DecrementAndMaybeResume,
                CompletionOutcome::Failure { .. } => &completion::FailWaitGroup,
            },
            // WaitGroupBarrier has no step-mode completion.
            Self::WaitGroupBarrier => &completion::ScheduleNextBody,
        }
    }
}

pub mod body;
pub mod completion;
pub mod dispatch;
pub mod step;

#[cfg(test)]
mod tests {
    use super::StepDefId;
    use serde_json::json;

    #[test]
    fn stepdefid_from_metadata_recognizes_wait_group_child_new_field() {
        let meta = json!({
            "mode": "step",
            "step_name": "child-a",
            "wg_step_name": "__wg__all__abc"
        });

        assert_eq!(StepDefId::from_metadata(&meta), StepDefId::WaitGroupChild);
    }

    #[test]
    fn stepdefid_from_metadata_recognizes_wait_group_child_legacy_flag() {
        let meta = json!({
            "mode": "step",
            "step_name": "child-b",
            "is_wait_all_child": true
        });

        assert_eq!(StepDefId::from_metadata(&meta), StepDefId::WaitGroupChild);
    }

    #[test]
    fn stepdefid_from_metadata_parses_specialized_and_regular_step_types() {
        let sleep = json!({
            "mode": "step",
            "step_name": "__sleep",
            "step_type": "sleep"
        });
        let event = json!({
            "mode": "step",
            "step_name": "approval",
            "step_type": "wait_for_event"
        });
        let regular = json!({
            "mode": "step",
            "step_name": "step-one",
            "step_type": "step"
        });

        assert_eq!(StepDefId::from_metadata(&sleep), StepDefId::Sleep);
        assert_eq!(StepDefId::from_metadata(&event), StepDefId::WaitForEvent);
        assert_eq!(StepDefId::from_metadata(&regular), StepDefId::Step);
    }

    #[test]
    fn stepdefid_from_metadata_prefers_wait_group_child_over_step_type() {
        let meta = json!({
            "mode": "step",
            "step_name": "child-c",
            "step_type": "wait_for_event",
            "wg_step_name": "__wg__all__xyz"
        });

        assert_eq!(StepDefId::from_metadata(&meta), StepDefId::WaitGroupChild);
    }
}
