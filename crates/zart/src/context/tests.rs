//! Tests for context types. Compiled only in test mode.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;
use crate::retry::RetryConfig;
use crate::test_helpers::{Call, RecordingScheduler};
use scheduler::{DurableStorage, Scheduler, StorageBackend};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use super::task_context::TaskContext;
use super::step_context::StepContext;
use super::step_trait::ZartStep;
use super::state::{StepRecord, StepStatus, AttemptStatus, StepAttempt, ExecutionState};
mod tests {
    use super::*;
    use std::time::Duration;

    // ── Retry config serde ────────────────────────────────────────────────────

    #[test]
    fn retry_config_round_trips_through_json() {
        let cfg = RetryConfig::exponential(3, Duration::from_secs(2));
        let json = serde_json::to_string(&cfg).unwrap();
        let back: RetryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_attempts, 3);
        assert_eq!(back.initial_delay, Duration::from_secs(2));
        assert_eq!(back.backoff_multiplier, 2.0);
    }

    #[test]
    fn execution_state_with_attempts_round_trips_through_json() {
        let mut state = ExecutionState::default();
        state.steps.insert(
            "s".to_string(),
            StepRecord {
                status: StepStatus::Completed,
                result: Some(serde_json::json!(1)),
                in_task_id: None,
                retry_attempt: 1,
                retry_config: Some(RetryConfig::fixed(2, Duration::from_millis(500))),
                attempts: vec![
                    StepAttempt {
                        attempt_number: 1,
                        started_at: chrono::Utc::now(),
                        completed_at: Some(chrono::Utc::now()),
                        status: AttemptStatus::Failed,
                        error: Some("oops".to_string()),
                        result: None,
                    },
                    StepAttempt {
                        attempt_number: 2,
                        started_at: chrono::Utc::now(),
                        completed_at: Some(chrono::Utc::now()),
                        status: AttemptStatus::Completed,
                        error: None,
                        result: Some(serde_json::json!(1)),
                    },
                ],
            },
        );

        let json = serde_json::to_string(&state).unwrap();
        let back: ExecutionState = serde_json::from_str(&json).unwrap();
        let record = back.steps.get("s").unwrap();
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.retry_attempt, 1);
    }

    // ── wait_for_event ────────────────────────────────────────────────────────

    use crate::test_helpers::{Call, RecordingScheduler};

    /// First call in body mode: no step row in DB → schedule_at called with
    /// step_type=wait_for_event. Returns Scheduled.
    #[tokio::test]
    async fn body_mode_wait_for_event_first_call_schedules_step_task() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);
        let result: Result<serde_json::Value, _> = ctx
            .wait_for_event("approval", Some(Duration::from_secs(3600)))
            .await;

        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "approval"),
            "expected Scheduled, got: {result:?}"
        );

        let log = calls.lock().unwrap();
        let schedules: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(schedules.len(), 1, "exactly one schedule_at call");

        if let Call::ScheduleAt {
            task_id,
            metadata,
            execution_time,
            ..
        } = &schedules[0]
        {
            assert_eq!(task_id, "exec-1:step:approval");
            assert_eq!(metadata["step_type"], "wait_for_event");
            assert_eq!(metadata["step_name"], "approval");
            assert_eq!(metadata["run_id"], "exec-1");
            assert!(
                *execution_time > chrono::Utc::now(),
                "deadline must be in the future"
            );
        }
    }

    /// No timeout → execution_time is the maximum DateTime value.
    #[tokio::test]
    async fn body_mode_wait_for_event_no_timeout_uses_max_execution_time() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);
        let result: Result<serde_json::Value, _> = ctx.wait_for_event("no-deadline", None).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));

        let log = calls.lock().unwrap();
        let schedules: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(schedules.len(), 1);

        if let Call::ScheduleAt {
            execution_time,
            metadata,
            ..
        } = &schedules[0]
        {
            assert_eq!(
                *execution_time,
                chrono::DateTime::<chrono::Utc>::MAX_UTC,
                "no-timeout event step must use MAX_UTC"
            );
            assert_eq!(metadata["step_type"], "wait_for_event");
        }
    }

    /// When the step row is already completed in the DB, return the cached payload.
    #[tokio::test]
    async fn body_mode_wait_for_event_completed_returns_cached_payload() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "approved", serde_json::json!({"ok": true}))
            .build();
        let mut ctx = make_body_ctx(scheduler);
        let result: Result<serde_json::Value, _> = ctx.wait_for_event("approved", None).await;

        assert!(result.is_ok(), "should return Ok for completed event step");
        assert_eq!(result.unwrap()["ok"], true);
        let log = calls.lock().unwrap();
        assert!(
            log.iter().all(|c| !c.is_schedule_at()),
            "no schedule_at for cached event"
        );
    }

    /// In step mode, a completed event step returns the cached result.
    #[tokio::test]
    async fn step_mode_wait_for_event_returns_cached_payload() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "signed", serde_json::json!(42i32))
            .build();
        let mut ctx = make_step_ctx(scheduler, "other-step");
        let result: Result<i32, _> = ctx.wait_for_event("signed", None).await;

        assert_eq!(result.unwrap(), 42);
        let log = calls.lock().unwrap();
        assert!(log.iter().all(|c| !c.is_schedule_at()));
    }

    // ── New execution model: call-counting tests ──────────────────────────────

    fn make_body_ctx(scheduler: std::sync::Arc<dyn StorageBackend>) -> TaskContext {
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            "lock-tok",
            serde_json::json!({"input": "data"}),
        )
        .with_execution_mode(ExecutionMode::Body)
    }

    fn make_step_ctx(scheduler: std::sync::Arc<dyn StorageBackend>, target: &str) -> TaskContext {
        let task_id = format!("exec-1:step:{target}");
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            "lock-tok",
            serde_json::json!({"input": "data"}),
        )
        .with_task_id(task_id)
        .with_execution_mode(ExecutionMode::Step {
            target_step: target.to_string(),
            step_type: crate::execution_model::StepKind::Step,
            retry_attempt: 0,
            retry_config: None,
        })
    }

    // ── Helper ZartStep structs for tests ─────────────────────────────────────

    struct ChargeCardStep;

    #[async_trait::async_trait]
    impl ZartStep for ChargeCardStep {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("charge-card")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(99)
        }
    }

    struct ChargeCardStepWithResult {
        result: u32,
    }

    #[async_trait::async_trait]
    impl ZartStep for ChargeCardStepWithResult {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("charge-card")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(self.result)
        }
    }

    struct FailingChargeCardStep;

    #[async_trait::async_trait]
    impl ZartStep for FailingChargeCardStep {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("charge-card")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Err(StepError::Failed {
                step: "charge-card".to_string(),
                reason: "card declined".to_string(),
            })
        }
    }

    struct StepOne;

    #[async_trait::async_trait]
    impl ZartStep for StepOne {
        type Output = i32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-one")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(21)
        }
    }

    struct StepA;
    struct StepB;
    struct StepC;

    #[async_trait::async_trait]
    impl ZartStep for StepA {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-a")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(1)
        }
    }
    #[async_trait::async_trait]
    impl ZartStep for StepB {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-b")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(2)
        }
    }
    #[async_trait::async_trait]
    impl ZartStep for StepC {
        type Output = u32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-c")
        }
        async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
            Ok(3)
        }
    }

    // ── body mode: execute_step ───────────────────────────────────────────────

    #[tokio::test]
    async fn body_mode_first_step_inserts_exactly_one_task_row() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);

        let result = ctx.execute_step(ChargeCardStep).await;

        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "charge-card"),
            "first step call in body mode must return Scheduled"
        );

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(inserts.len(), 1, "exactly one task row inserted");

        if let Call::ScheduleAt {
            task_id, metadata, ..
        } = &inserts[0]
        {
            assert_eq!(task_id, "exec-1:step:charge-card");
            assert_eq!(metadata["mode"], "step");
            assert_eq!(metadata["step_type"], "step");
            assert_eq!(metadata["step_name"], "charge-card");
            assert_eq!(metadata["run_id"], "exec-1");
        } else {
            panic!("unexpected call variant");
        }
    }

    #[tokio::test]
    async fn body_mode_completed_step_returns_cached_result_with_zero_db_writes() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "charge-card", serde_json::json!(42))
            .build();
        let mut ctx = make_body_ctx(scheduler);

        let result: Result<u32, _> = ctx
            .execute_step(ChargeCardStepWithResult { result: 0 })
            .await;

        assert_eq!(result.unwrap(), 42, "cached result must be returned");

        let log = calls.lock().unwrap();
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            0
        );
    }

    #[tokio::test]
    async fn body_mode_inflight_step_returns_scheduled_without_inserting_duplicate() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_in_flight("exec-1", "charge-card")
            .build();
        let mut ctx = make_body_ctx(scheduler);

        let result = ctx.execute_step(ChargeCardStep).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));
        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_schedule_at()).count(),
            0,
            "step row already exists; must not insert a duplicate"
        );
    }

    // ── step mode: target and non-target steps ────────────────────────────────

    #[tokio::test]
    async fn step_mode_target_step_executes_lambda_and_atomically_completes() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx(scheduler, "charge-card");

        let result: Result<u32, _> = ctx.execute_step(ChargeCardStep).await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "target step must return StepExecuted (transactional completion)"
        );

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_schedule_at()).count(),
            0,
            "no new rows in step mode"
        );

        let cas: Vec<_> = log
            .iter()
            .filter(|c| c.is_complete_and_schedule())
            .collect();
        assert_eq!(cas.len(), 1, "complete_and_schedule called exactly once");

        if let Call::CompleteAndSchedule {
            completed_task_id,
            new_task_id,
            new_metadata,
            ..
        } = &cas[0]
        {
            assert_eq!(completed_task_id, "exec-1:step:charge-card");
            // Body task ID is now a UUID, not a deterministic segment-based ID.
            assert!(!new_task_id.is_empty(), "body task ID must be set");
            assert_eq!(new_metadata["mode"], "body");
            assert_eq!(new_metadata["run_id"], "exec-1");
        } else {
            panic!("unexpected call variant");
        }
    }

    #[tokio::test]
    async fn step_mode_nontarget_step_reads_cache_with_zero_writes() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "step-one", serde_json::json!(21))
            .build();
        let mut ctx = make_step_ctx(scheduler, "step-two");

        let result: Result<i32, _> = ctx.execute_step(StepOne).await;

        assert_eq!(result.unwrap(), 21, "should return the cached result");

        let log = calls.lock().unwrap();
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            0
        );
    }

    // ── body mode: wait_all ───────────────────────────────────────────────────

    #[tokio::test]
    async fn wait_all_body_mode_n_unscheduled_steps_creates_n_children_plus_one_coordinator() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);

        let h1 = ctx.schedule_step(StepA);
        let h2 = ctx.schedule_step(StepB);
        let h3 = ctx.schedule_step(StepC);
        let result = ctx.wait_all(vec![h1, h2, h3]).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));

        let log = calls.lock().unwrap();
        let inserts: Vec<&serde_json::Value> = log
            .iter()
            .filter_map(|c| {
                if let Call::ScheduleAt { metadata, .. } = c {
                    Some(metadata)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(
            inserts.len(),
            4,
            "3 child step rows + 1 coordinator = 4 total inserts"
        );

        let children: Vec<_> = inserts
            .iter()
            .filter(|m| {
                m.get("is_wait_all_child")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            children.len(),
            3,
            "three children each marked is_wait_all_child=true"
        );

        let coordinators: Vec<_> = inserts
            .iter()
            .filter(|m| m["step_type"] == "wait_all")
            .collect();
        assert_eq!(coordinators.len(), 1, "exactly one coordinator task");
        assert_eq!(
            coordinators[0]["run_id"], "exec-1",
            "coordinator must carry run_id in metadata"
        );
        assert_eq!(coordinators[0]["mode"], "step");
    }

    #[tokio::test]
    async fn wait_all_body_mode_all_completed_returns_results_with_zero_new_tasks() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "step-a", serde_json::json!(10))
            .step_completed("exec-1", "step-b", serde_json::json!(20))
            .build();
        let mut ctx = make_body_ctx(scheduler);

        struct CachedStepA;
        #[async_trait::async_trait]
        impl ZartStep for CachedStepA {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("step-a")
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(99)
            }
        }
        struct CachedStepB;
        #[async_trait::async_trait]
        impl ZartStep for CachedStepB {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("step-b")
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(99)
            }
        }

        let h1 = ctx.schedule_step(CachedStepA);
        let h2 = ctx.schedule_step(CachedStepB);
        let results = ctx.wait_all(vec![h1, h2]).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(*results[0].as_ref().unwrap(), 10u32);
        assert_eq!(*results[1].as_ref().unwrap(), 20u32);

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_schedule_at()).count(),
            0,
            "all steps already completed — no new rows inserted"
        );
        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            0
        );
    }

    // ── step mode: wait_all child execution ───────────────────────────────────

    #[tokio::test]
    async fn wait_all_step_mode_target_child_calls_mark_completed_once_not_complete_and_schedule() {
        let (scheduler, calls) = RecordingScheduler::builder().build();

        let mut ctx = TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            "lock-tok",
            serde_json::json!({}),
        )
        .with_task_id("exec-1:step:step-b".to_string())
        .with_execution_mode(ExecutionMode::Step {
            target_step: "step-b".to_string(),
            step_type: crate::execution_model::StepKind::Step,
            retry_attempt: 0,
            retry_config: None,
        });

        struct WaitAllStepA;
        struct WaitAllStepB;
        struct WaitAllStepC;
        #[async_trait::async_trait]
        impl ZartStep for WaitAllStepA {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("step-a")
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(0)
            }
        }
        #[async_trait::async_trait]
        impl ZartStep for WaitAllStepB {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("step-b")
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(2)
            }
        }
        #[async_trait::async_trait]
        impl ZartStep for WaitAllStepC {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("step-c")
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(0)
            }
        }

        let h1 = ctx.schedule_step(WaitAllStepA);
        let h2 = ctx.schedule_step(WaitAllStepB);
        let h3 = ctx.schedule_step(WaitAllStepC);
        let result = ctx.wait_all(vec![h1, h2, h3]).await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "step-b"),
            "wait_all child must return StepExecuted"
        );

        let log = calls.lock().unwrap();

        let mc: Vec<_> = log
            .iter()
            .filter_map(|c| {
                if let Call::MarkCompleted { task_id, .. } = c {
                    Some(task_id.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            mc.len(),
            1,
            "complete_step_no_resume delegates to mark_completed once"
        );
        assert_eq!(mc[0], "exec-1:step:step-b");

        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            0,
            "coordinator handles body scheduling; wait_all children must NOT call complete_and_schedule"
        );
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
    }

    // ── body mode: sleep ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn sleep_body_mode_inserts_one_sleep_task_with_exact_wake_time() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);

        let wake_time = chrono::Utc::now() + chrono::Duration::hours(1);
        let result = ctx.sleep_until(wake_time).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log
            .iter()
            .filter_map(|c| {
                if let Call::ScheduleAt {
                    task_id,
                    execution_time,
                    metadata,
                    ..
                } = c
                {
                    Some((task_id, execution_time, metadata))
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(inserts.len(), 1, "sleep inserts exactly one task row");
        let (task_id, exec_time, meta) = inserts[0];
        // Sleep task ID is now a UUID, not a deterministic segment-based ID.
        assert!(!task_id.is_empty(), "sleep task ID must be set");
        assert_eq!(meta["mode"], "step");
        assert_eq!(meta["step_type"], "sleep");
        assert_eq!(meta["run_id"], "exec-1");
        let diff = (*exec_time - wake_time).num_seconds().abs();
        assert!(
            diff < 1,
            "sleep task execution_time must equal the requested wake_time"
        );
    }

    // ── execute_step with retry: new execution model ──────────────────────────

    /// Helper: make a step-mode context with a retry config embedded.
    fn make_step_ctx_with_retry(
        scheduler: std::sync::Arc<dyn StorageBackend>,
        target: &str,
        retry_attempt: usize,
        retry_config: RetryConfig,
    ) -> TaskContext {
        let task_id = format!("exec-1:step:{target}");
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            "lock-tok",
            serde_json::json!({}),
        )
        .with_task_id(task_id)
        .with_execution_mode(ExecutionMode::Step {
            target_step: target.to_string(),
            step_type: crate::execution_model::StepKind::Step,
            retry_attempt,
            retry_config: Some(retry_config),
        })
    }

    /// In body mode, `execute_step` must embed the retry_config in the
    /// scheduled step task's metadata so the step task can retry on failure.
    #[tokio::test]
    async fn body_mode_execute_step_with_retry_embeds_retry_config_in_metadata() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler);

        struct RetryStep;
        #[async_trait::async_trait]
        impl ZartStep for RetryStep {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("charge-card")
            }
            fn retry_config(&self) -> Option<RetryConfig> {
                Some(RetryConfig::fixed(3, Duration::from_secs(5)))
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(99)
            }
        }

        let result = ctx.execute_step(RetryStep).await;

        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "charge-card"),
            "execute_step with retry in body mode returns Scheduled on first call"
        );

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(inserts.len(), 1, "exactly one task row inserted");

        if let Call::ScheduleAt { metadata, .. } = &inserts[0] {
            assert!(
                metadata.get("retry_config").is_some(),
                "retry_config must be present in step task metadata"
            );
            let embedded: RetryConfig =
                serde_json::from_value(metadata["retry_config"].clone()).unwrap();
            assert_eq!(embedded.max_attempts, 3);
        }
    }

    /// When the step fails and retries remain, `step_step_mode` must call
    /// `mark_failed` with a future execution time and return `StepExecuted`.
    #[tokio::test]
    async fn step_mode_failure_with_retries_remaining_schedules_retry_via_mark_failed() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            0,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx.execute_step(FailingChargeCardStep).await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "must return StepExecuted so the worker skips its own mark_failed"
        );

        let log = calls.lock().unwrap();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();
        assert_eq!(
            failures.len(),
            1,
            "exactly one mark_failed call for the retry"
        );

        if let Call::MarkFailed {
            task_id,
            next_execution_time,
            ..
        } = &failures[0]
        {
            assert_eq!(task_id, "exec-1:step:charge-card");
            assert!(
                next_execution_time.is_some(),
                "retry must carry a future execution_time for the delay"
            );
            let delay_secs =
                (*next_execution_time.as_ref().unwrap() - chrono::Utc::now()).num_seconds();
            assert!(delay_secs > 0, "retry must be in the future");
        }
    }

    /// When all retries are exhausted the original error propagates and
    /// `mark_failed` is NOT called.
    #[tokio::test]
    async fn step_mode_failure_with_retries_exhausted_propagates_error() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            3,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx.execute_step(FailingChargeCardStep).await;

        assert!(
            matches!(result, Err(StepError::Failed { .. })),
            "error must propagate when retries are exhausted"
        );

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_mark_failed()).count(),
            0,
            "step_step_mode must not call mark_failed when retries are exhausted"
        );
    }

    /// A successful step in step mode must NOT trigger a retry.
    #[tokio::test]
    async fn step_mode_success_with_retry_config_completes_normally() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            0,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx.execute_step(ChargeCardStep).await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "successful step must return StepExecuted"
        );

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_mark_failed()).count(),
            0,
            "no mark_failed on success"
        );
        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            1,
            "complete_and_schedule called once to commit step and schedule next body"
        );
    }

    // ── Step name uniqueness ──────────────────────────────────────────────────

    /// Two sequential `execute_step` calls with unique names on a body re-run must each
    /// return their own cached result — the correct behaviour for loops with unique step names.
    #[tokio::test]
    async fn body_mode_loop_with_unique_names_returns_correct_per_iteration_result() {
        let (scheduler, _calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "loop-item-0", serde_json::json!(10u32))
            .step_completed("exec-1", "loop-item-1", serde_json::json!(20u32))
            .build();
        let mut ctx = make_body_ctx(scheduler);

        struct LoopItemStep {
            index: usize,
        }
        #[async_trait::async_trait]
        impl ZartStep for LoopItemStep {
            type Output = u32;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Owned(format!("loop-item-{}", self.index))
            }
            async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
                Ok(0)
            }
        }

        let r0 = ctx.execute_step(LoopItemStep { index: 0 }).await.unwrap();
        let r1 = ctx.execute_step(LoopItemStep { index: 1 }).await.unwrap();

        assert_eq!(r0, 10u32, "iteration 0 must return its own cached value");
        assert_eq!(r1, 20u32, "iteration 1 must return its own cached value");
    }

    /// Using `.with_id()` at the call site produces the same unique-name guarantee.
    #[tokio::test]
    async fn body_mode_loop_with_id_override_returns_correct_per_iteration_result() {
        let (scheduler, _calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "process-item-0", serde_json::json!(100u32))
            .step_completed("exec-1", "process-item-1", serde_json::json!(200u32))
            .build();
        let mut ctx = make_body_ctx(scheduler);

        let r0 = ctx
            .execute_step(ChargeCardStep.with_id("process-item-0"))
            .await
            .unwrap();
        let r1 = ctx
            .execute_step(ChargeCardStep.with_id("process-item-1"))
            .await
            .unwrap();

        assert_eq!(r0, 100u32);
        assert_eq!(r1, 200u32);
    }

    /// Calling `execute_step` twice with the same step name in a body re-run must return
    /// an error rather than silently returning stale cached data for the second call.
    /// The DB `task_id PRIMARY KEY` prevents duplicate rows at INSERT time, but on re-run
    /// cached Completed results are returned without inserting — this guard catches that.
    #[tokio::test]
    async fn body_mode_duplicate_step_name_in_loop_returns_error() {
        let (scheduler, _calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "charge-card", serde_json::json!(99u32))
            .build();
        let mut ctx = make_body_ctx(scheduler);

        // First call: returns the cached value correctly.
        let first = ctx.execute_step(ChargeCardStep).await;
        assert!(first.is_ok(), "first call must succeed: {first:?}");

        // Second call with the same name: must error rather than return stale data.
        let second = ctx.execute_step(ChargeCardStep).await;
        assert!(
            matches!(
                second,
                Err(StepError::Failed { ref step, ref reason })
                    if step == "charge-card" && reason.contains("duplicate step name")
            ),
            "second call with duplicate step name must fail with a clear error, got: {second:?}"
        );
    }
}
