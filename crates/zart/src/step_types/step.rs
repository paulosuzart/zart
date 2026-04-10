//! Step-mode behavior implementations for declarative step dispatch.
//!
//! This file provides the step-behavior layer only. Completion logic remains
//! in `completion.rs` and is intentionally not called from here.

use async_trait::async_trait;
use scheduler::{StepLookup, TaskStatus};

use crate::context::{PendingFn, TaskContext};
use crate::error::StepError;
use crate::step_types::{StepBehavior, StepResult};

/// Step behavior for regular executable steps and wait-group child steps.
///
/// Behavior:
/// - If this is the target step (lambda provided), execute lambda and return
///   `StepResult::Executed`.
/// - If lambda is absent, read cached completed result and return
///   `StepResult::Cached`.
/// - If step is not completed and lambda absent, return `StepError::Scheduled`.
pub struct ExecuteLambda;

#[async_trait]
impl StepBehavior for ExecuteLambda {
    async fn handle(
        &self,
        ctx: &TaskContext,
        step_name: &str,
        lambda: Option<PendingFn>,
    ) -> Result<StepResult, StepError> {
        if let Some(pending) = lambda {
            let json = crate::local::ZART_PHASE
                .scope(crate::local::Phase::Step(ctx.step_context()), pending())
                .await?;
            return Ok(StepResult::Executed(json));
        }

        let lookup = ctx
            .scheduler
            .get_step_status(ctx.run_id(), step_name)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => Ok(StepResult::Cached(json)),
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step completed but result is missing".to_string(),
            }),
            _ => Err(StepError::Scheduled {
                step: step_name.to_string(),
                next_execution: None,
            }),
        }
    }
}

/// Step behavior for transition-only steps (e.g. sleep).
///
/// No lambda is executed; the completion layer decides what to schedule next.
pub struct TransitionOnly;

#[async_trait]
impl StepBehavior for TransitionOnly {
    async fn handle(
        &self,
        _ctx: &TaskContext,
        _step_name: &str,
        _lambda: Option<PendingFn>,
    ) -> Result<StepResult, StepError> {
        Ok(StepResult::Transition)
    }
}

/// Step behavior for wait_for_event deadline task path.
///
/// In step mode, this behavior only checks cached result:
/// - completed with payload -> `Cached`
/// - completed without payload -> error
/// - otherwise -> `Scheduled` (caller may route to timeout completion)
pub struct LookupCached;

#[async_trait]
impl StepBehavior for LookupCached {
    async fn handle(
        &self,
        ctx: &TaskContext,
        step_name: &str,
        _lambda: Option<PendingFn>,
    ) -> Result<StepResult, StepError> {
        let lookup = ctx
            .scheduler
            .get_step_status(ctx.run_id(), step_name)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => Ok(StepResult::Cached(json)),
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "event step completed but result is missing".to_string(),
            }),
            _ => Err(StepError::DeadlineExceeded {
                step: step_name.to_string(),
            }),
        }
    }
}
