/// Admin retry step tests.
use super::helpers::*;
use uuid::Uuid;

/// Tests that `admin_retry_step` clears the persisted step deadline so the
/// retried step can actually execute (instead of immediately timing out).
///
/// Scenario:
/// 1. A step with a very short timeout (1 ms) times out and becomes Dead.
/// 2. The step row carries `metadata["deadline"]` (a past RFC3339 timestamp).
/// 3. `admin_retry_step` copies the old task metadata but removes `"deadline"`.
/// 4. The retried step picks up with no deadline → runs normally.
#[tokio::test]
#[ignore]
async fn admin_retry_step_clears_deadline_so_retried_step_can_run() {
    let scheduler = setup().await;

    let execution_id = format!("admin-deadline-retry-{}", Uuid::new_v4());
    let run_id = format!("{execution_id}:run:0");

    scheduler
        .start_execution(&execution_id, "test-task", serde_json::json!({}))
        .await
        .expect("start_execution failed");

    let pool = sqlx::PgPool::connect(&pg_url())
        .await
        .expect("failed to connect to PostgreSQL");

    let step_task_id = format!("{run_id}:step:slow-step");
    let past_deadline = chrono::Utc::now() - chrono::Duration::seconds(10);
    let step_metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "run_id": run_id,
        "execution_id": execution_id,
        "step_name": "slow-step",
        "retry_attempt": 0,
        "deadline": past_deadline.to_rfc3339(),
    });

    sqlx::query(
        r#"
        INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata, status, attempt)
        VALUES ($1, 'test-task', NOW(), $2, $3, 'completed', 1)
        "#,
    )
    .bind(&step_task_id)
    .bind(serde_json::json!({}))
    .bind(&step_metadata)
    .execute(&pool)
    .await
    .expect("insert task failed");

    sqlx::query(
        r#"
        INSERT INTO zart_steps (step_id, run_id, step_name, task_id, status, step_kind, retry_attempt)
        VALUES ($1, $2, $3, $4, 'dead', 'step', 3)
        "#,
    )
    .bind(format!("{run_id}:step:slow-step"))
    .bind(&run_id)
    .bind("slow-step")
    .bind(&step_task_id)
    .execute(&pool)
    .await
    .expect("insert step failed");

    let new_task_id = scheduler
        .retry_dead_step(&run_id, "slow-step", Some("test"))
        .await
        .expect("retry_dead_step failed");

    let new_metadata: Option<serde_json::Value> =
        sqlx::query_scalar(r#"SELECT metadata FROM zart_tasks WHERE task_id = $1"#)
            .bind(&new_task_id)
            .fetch_one(&pool)
            .await
            .expect("query new task metadata failed");

    let meta = new_metadata.expect("new task should have metadata");
    assert!(
        meta.get("deadline").is_none(),
        "retry_dead_step should have removed the 'deadline' key, but got: {meta}"
    );

    let step_status: Option<String> = sqlx::query_scalar(
        r#"SELECT status::text FROM zart_steps WHERE step_name = $1 AND run_id = $2"#,
    )
    .bind("slow-step")
    .bind(&run_id)
    .fetch_one(&pool)
    .await
    .expect("query step status failed");

    assert_eq!(
        step_status,
        Some("scheduled".to_string()),
        "step should be scheduled after admin retry"
    );
}
