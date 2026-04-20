/// Declarative dispatch and step_internal integration tests.
use super::helpers::*;
use std::time::Duration;
use uuid::Uuid;
use zart::step_types::{CompletionBehavior, CompletionOutcome, CompletionSpec, StepResult};
use zart::{DurableScheduler, TaskRegistry, step_types::StepDefId};
use zart_core::TaskMetadata;
use zart_core::store::{EventStore as _, StepStore as _, WaitGroupStore as _};
use zart_core::types::{
    CompleteWaitGroupChildParams, FailWaitGroupChildParams, ScheduleStepParams, StepKind,
    UpsertWaitGroupStepParams,
};

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn stepdefid_from_task_metadata_correct() {
    use zart_core::TaskMetadata;
    use zart_core::task_metadata::StepMetaType;

    let make = |step_type: StepMetaType,
                step_name: &str,
                wg_step_name: Option<&str>,
                is_wait_all_child: Option<bool>|
     -> TaskMetadata {
        TaskMetadata::Step {
            step_type,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: step_name.into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child,
            wg_step_name: wg_step_name.map(str::to_string),
        }
    };

    let wg_new = make(StepMetaType::Step, "child-a", Some("__wg__all__abc"), None);
    let wg_old = make(StepMetaType::Step, "child-b", None, Some(true));
    let sleep = make(StepMetaType::Sleep, "__sleep", None, None);
    let event = make(StepMetaType::WaitForEvent, "approval", None, None);
    let regular = make(StepMetaType::Step, "step-one", None, None);

    assert_eq!(
        StepDefId::from_task_metadata(&wg_new),
        StepDefId::WaitGroupChild
    );
    assert_eq!(
        StepDefId::from_task_metadata(&wg_old),
        StepDefId::WaitGroupChild
    );
    assert_eq!(StepDefId::from_task_metadata(&sleep), StepDefId::Sleep);
    assert_eq!(
        StepDefId::from_task_metadata(&event),
        StepDefId::WaitForEvent
    );
    assert_eq!(StepDefId::from_task_metadata(&regular), StepDefId::Step);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn wait_group_complete_concurrent_schedules_body_once() {
    let scheduler = setup().await;

    let execution_id = format!("test-wg-concurrent-{}", Uuid::new_v4());
    let run_id = format!("{execution_id}:run:0");
    let task_name = "wg-task";

    scheduler
        .start_execution(&execution_id, task_name, serde_json::json!({}))
        .await
        .expect("start_execution failed");

    scheduler
        .upsert_wait_group_step(UpsertWaitGroupStepParams {
            run_id: run_id.clone(),
            group_step_name: "__wg__all__concurrent".to_string(),
            total: 2,
            threshold: 0,
        })
        .await
        .expect("upsert_wait_group_step failed");

    let child1_task_id = format!("{run_id}:step:child-1");
    let child2_task_id = format!("{run_id}:step:child-2");

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: child1_task_id.clone(),
            task_name: task_name.to_string(),
            run_id: run_id.clone(),
            step_name: "child-1".to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            metadata: serde_json::json!({
                "mode": "step",
                "step_type": "step",
                "run_id": run_id.clone(),
                "execution_id": execution_id.clone(),
                "step_name": "child-1",
                "is_wait_all_child": true,
                "wg_step_name": "__wg__all__concurrent"
            }),
            retry_config: None,
        })
        .await
        .expect("schedule child-1 failed");

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: child2_task_id.clone(),
            task_name: task_name.to_string(),
            run_id: run_id.clone(),
            step_name: "child-2".to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            metadata: serde_json::json!({
                "mode": "step",
                "step_type": "step",
                "run_id": run_id.clone(),
                "execution_id": execution_id.clone(),
                "step_name": "child-2",
                "is_wait_all_child": true,
                "wg_step_name": "__wg__all__concurrent"
            }),
            retry_config: None,
        })
        .await
        .expect("schedule child-2 failed");

    let fetched = scheduler
        .poll_due(chrono::Utc::now(), 200)
        .await
        .expect("poll_due failed");

    let lock1 = fetched
        .iter()
        .find(|t| t.task_id == child1_task_id)
        .map(|t| t.lock_token.clone())
        .expect("child-1 task not fetched");
    let lock2 = fetched
        .iter()
        .find(|t| t.task_id == child2_task_id)
        .map(|t| t.lock_token.clone())
        .expect("child-2 task not fetched");

    let next_body_task_id = format!("{run_id}:body:after:__wg__all__concurrent");

    let s1 = scheduler.clone();
    let run_id_1 = run_id.clone();
    let child1_task_id_clone = child1_task_id.clone();
    let next_body_task_id_1 = next_body_task_id.clone();
    let execution_id_1 = execution_id.clone();
    let child1 = tokio::spawn(async move {
        s1.complete_wait_group_child(CompleteWaitGroupChildParams {
            run_id: run_id_1,
            execution_id: execution_id_1,
            group_step_name: "__wg__all__concurrent".to_string(),
            child_step_task_id: child1_task_id_clone.clone(),
            child_step_id: child1_task_id_clone,
            child_result: serde_json::json!(1),
            lock_token: lock1,
            attempt_number: 1,
            next_body_task_id: next_body_task_id_1,
            task_name: task_name.to_string(),
            data: serde_json::json!({}),
        })
        .await
    });

    let s2 = scheduler.clone();
    let run_id_2 = run_id.clone();
    let execution_id_2 = execution_id.clone();
    let child2_task_id_clone = child2_task_id.clone();
    let next_body_task_id_2 = next_body_task_id.clone();
    let child2 = tokio::spawn(async move {
        s2.complete_wait_group_child(CompleteWaitGroupChildParams {
            run_id: run_id_2,
            execution_id: execution_id_2,
            group_step_name: "__wg__all__concurrent".to_string(),
            child_step_task_id: child2_task_id_clone.clone(),
            child_step_id: child2_task_id_clone,
            child_result: serde_json::json!(2),
            lock_token: lock2,
            attempt_number: 1,
            next_body_task_id: next_body_task_id_2,
            task_name: task_name.to_string(),
            data: serde_json::json!({}),
        })
        .await
    });

    let r1: Result<bool, zart_core::StorageError> = child1.await.expect("join child1 failed");
    let r2: Result<bool, zart_core::StorageError> = child2.await.expect("join child2 failed");
    let t1 = r1.expect("complete_wait_group_child #1 failed");
    let t2 = r2.expect("complete_wait_group_child #2 failed");

    assert!(t1 ^ t2, "exactly one child should trigger body scheduling");

    let fetched = scheduler
        .poll_due(chrono::Utc::now(), 200)
        .await
        .expect("poll_due failed");
    let body_count = fetched
        .iter()
        .filter(|t| t.task_id == next_body_task_id)
        .count();
    assert_eq!(body_count, 1, "body must be scheduled exactly once");
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn wait_group_failure_first_only_fails_execution_once() {
    let scheduler = setup().await;

    let execution_id = format!("test-wg-fail-{}", Uuid::new_v4());
    let run_id = format!("{execution_id}:run:0");
    let task_name = "wg-fail-task";

    scheduler
        .start_execution(&execution_id, task_name, serde_json::json!({}))
        .await
        .expect("start_execution failed");

    scheduler
        .upsert_wait_group_step(UpsertWaitGroupStepParams {
            run_id: run_id.clone(),
            group_step_name: "__wg__all__fail".to_string(),
            total: 3,
            threshold: 0,
        })
        .await
        .expect("upsert_wait_group_step failed");

    let fail1_task_id = format!("{run_id}:step:fail-1");
    let fail2_task_id = format!("{run_id}:step:fail-2");

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: fail1_task_id.clone(),
            task_name: task_name.to_string(),
            run_id: run_id.clone(),
            step_name: "fail-1".to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            metadata: serde_json::json!({
                "mode": "step",
                "step_type": "step",
                "run_id": run_id.clone(),
                "execution_id": execution_id.clone(),
                "step_name": "fail-1",
                "is_wait_all_child": true,
                "wg_step_name": "__wg__all__fail"
            }),
            retry_config: None,
        })
        .await
        .expect("schedule fail-1 failed");

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: fail2_task_id.clone(),
            task_name: task_name.to_string(),
            run_id: run_id.clone(),
            step_name: "fail-2".to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            metadata: serde_json::json!({
                "mode": "step",
                "step_type": "step",
                "run_id": run_id.clone(),
                "execution_id": execution_id.clone(),
                "step_name": "fail-2",
                "is_wait_all_child": true,
                "wg_step_name": "__wg__all__fail"
            }),
            retry_config: None,
        })
        .await
        .expect("schedule fail-2 failed");

    let fetched = scheduler
        .poll_due(chrono::Utc::now(), 200)
        .await
        .expect("poll_due failed");

    let fail1_lock = fetched
        .iter()
        .find(|t| t.task_id == fail1_task_id)
        .map(|t| t.lock_token.clone())
        .expect("fail-1 task not fetched");
    let fail2_lock = fetched
        .iter()
        .find(|t| t.task_id == fail2_task_id)
        .map(|t| t.lock_token.clone())
        .expect("fail-2 task not fetched");

    let first = scheduler
        .fail_wait_group_child(FailWaitGroupChildParams {
            run_id: run_id.clone(),
            group_step_name: "__wg__all__fail".to_string(),
            child_step_task_id: fail1_task_id.clone(),
            child_step_id: fail1_task_id,
            error: "boom-1".to_string(),
            lock_token: fail1_lock,
            attempt_number: 1,
        })
        .await
        .expect("fail_wait_group_child first failed");

    let second = scheduler
        .fail_wait_group_child(FailWaitGroupChildParams {
            run_id: run_id.clone(),
            group_step_name: "__wg__all__fail".to_string(),
            child_step_task_id: fail2_task_id.clone(),
            child_step_id: fail2_task_id,
            error: "boom-2".to_string(),
            lock_token: fail2_lock,
            attempt_number: 1,
        })
        .await
        .expect("fail_wait_group_child second failed");

    assert!(first, "first failure must win CAS");
    assert!(!second, "second failure must not win CAS");

    if first {
        scheduler
            .fail_execution(&execution_id)
            .await
            .expect("fail_execution failed");
    }
    let exec = scheduler
        .get_execution(&execution_id)
        .await
        .expect("get_execution failed")
        .expect("execution not found");
    assert_eq!(exec.status, ExecutionStatus::Failed);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn deliver_event_happy_path_and_idempotency() {
    let scheduler = setup().await;
    let durable = DurableScheduler::new(scheduler.clone());

    let execution_id = format!("test-deliver-event-{}", Uuid::new_v4());
    durable
        .start(&execution_id, "wait-event-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let mut registry = TaskRegistry::new();
    registry.register("wait-event-task", WaitEventTask);
    let registry = Arc::new(registry);
    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    tokio::time::sleep(Duration::from_millis(600)).await;

    let r1 = scheduler
        .deliver_event(
            &execution_id,
            "approve",
            serde_json::json!({ "approved": true }),
        )
        .await
        .expect("deliver_event #1 failed");
    let r2 = scheduler
        .deliver_event(
            &execution_id,
            "approve",
            serde_json::json!({ "approved": true }),
        )
        .await
        .expect("deliver_event #2 failed");

    assert_eq!(r1, EventDeliveryResult::Delivered);
    assert_eq!(r2, EventDeliveryResult::AlreadyDelivered);

    let record = durable
        .wait(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    assert_eq!(record.result.expect("result missing")["approved"], true);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn completion_behaviors_execute_with_real_backend() {
    let scheduler = setup().await;

    let execution_id = format!("test-completion-{}", Uuid::new_v4());
    let run_id = format!("{execution_id}:run:0");
    let task_name = "completion-task";

    scheduler
        .start_execution(&execution_id, task_name, serde_json::json!({}))
        .await
        .expect("start_execution failed");

    let schedule = scheduler
        .schedule_step(ScheduleStepParams {
            task_id: format!("{execution_id}:step:comp-step"),
            task_name: task_name.to_string(),
            run_id: run_id.clone(),
            step_name: "comp-step".to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            metadata: serde_json::json!({
                "mode": "step",
                "step_type": "step",
                "run_id": run_id,
                "execution_id": execution_id,
                "step_name": "comp-step"
            }),
            retry_config: None,
        })
        .await
        .expect("schedule_step failed");

    let fetched = scheduler
        .poll_due(chrono::Utc::now(), 200)
        .await
        .expect("poll_due failed");

    let step_lock = fetched
        .iter()
        .find(|t| t.task_id == schedule.task_id)
        .map(|t| t.lock_token.clone())
        .expect("scheduled step task not fetched");

    let spec = CompletionSpec {
        step_task_id: schedule.task_id.clone(),
        step_id: schedule.task_id.clone(),
        step_name: "comp-step".to_string(),
        worker_id: step_lock,
        task_name: task_name.to_string(),
        run_id: format!("{execution_id}:run:0"),
        execution_id: execution_id.clone(),
        data: serde_json::json!({}),
        attempt_number: 1,
        result: StepResult::Executed(serde_json::json!({"ok": true})),
        wait_group_step_name: None,
        outcome: CompletionOutcome::Success,
    };

    let behavior = zart::step_types::completion::ScheduleNextBody;
    behavior
        .complete(&*scheduler, spec)
        .await
        .expect("ScheduleNextBody::complete failed");

    let due = scheduler
        .poll_due(chrono::Utc::now(), 200)
        .await
        .expect("poll_due failed");

    let body_scheduled = due.iter().any(|t| {
        matches!(
            TaskMetadata::from_json_value(t.metadata.clone()),
            Ok(TaskMetadata::Body { .. })
        ) && t.task_id.contains(":body:after:comp-step")
    });
    assert!(body_scheduled, "expected body continuation task");
}
