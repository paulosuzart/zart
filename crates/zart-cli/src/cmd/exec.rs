use zart::{DurableScheduler, PgBackend};

pub async fn migrate(pool: sqlx::PgPool) {
    let pg = PgBackend::new(pool);
    pg.run_migrations().await.unwrap_or_else(|e| {
        eprintln!("error: migrations failed: {e}");
        std::process::exit(1);
    });
    println!("Migrations applied successfully.");
}

pub async fn schedule(
    durable: DurableScheduler,
    execution_id: Option<String>,
    task_name: String,
    data: String,
) {
    let payload: serde_json::Value = serde_json::from_str(&data).unwrap_or_else(|e| {
        eprintln!("error: invalid JSON payload: {e}");
        std::process::exit(1);
    });
    let execution_id = execution_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    durable
        .start(&execution_id, &task_name, payload)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to schedule execution: {e}");
            std::process::exit(1);
        });
    println!("Scheduled execution '{execution_id}' for task '{task_name}'.");
}

pub async fn status(durable: DurableScheduler, execution_id: String) {
    let record = durable.status(&execution_id).await.unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    println!("execution_id : {}", record.execution_id);
    println!("task_name    : {}", record.task_name);
    println!("status       : {}", record.status);
    println!("scheduled_at : {}", record.scheduled_at);
    if let Some(at) = record.completed_at {
        println!("completed_at : {at}");
    }
    if let Some(result) = record.result {
        println!("result       : {result}");
    }
}

pub async fn cancel(durable: DurableScheduler, execution_id: String) {
    let cancelled = durable.cancel(&execution_id).await.unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    if cancelled {
        println!("Execution '{execution_id}' cancelled.");
    } else {
        eprintln!("Execution '{execution_id}' not found or already in a terminal state.");
        std::process::exit(1);
    }
}

pub async fn wait(durable: DurableScheduler, execution_id: String, timeout_secs: u64) {
    let record = durable
        .wait(
            &execution_id,
            std::time::Duration::from_secs(timeout_secs),
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!("execution_id : {}", record.execution_id);
    println!("status       : {}", record.status);
    if let Some(at) = record.completed_at {
        println!("completed_at : {at}");
    }
    if let Some(result) = record.result {
        println!("result       : {result}");
    }
}
