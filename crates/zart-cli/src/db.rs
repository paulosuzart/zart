use std::sync::Arc;
use zart::DurableScheduler;

/// Return a raw connection pool (used by `migrate`).
pub async fn pool(db_url: Option<String>) -> sqlx::PgPool {
    let url = require_db_url(db_url);
    connect(&url).await
}

/// Connect without pause storage (used by execution lifecycle commands).
pub async fn simple(db_url: Option<String>) -> DurableScheduler {
    let url = require_db_url(db_url);
    let pool = connect(&url).await;
    let scheduler = Arc::new(zart::PostgresStorage::new(pool));
    DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler())
}

/// Connect with pause storage enabled (used by all admin commands).
pub async fn admin(db_url: Option<String>) -> DurableScheduler {
    let url = require_db_url(db_url);
    let pool = connect(&url).await;
    let scheduler = Arc::new(zart::PostgresStorage::new(pool));
    DurableScheduler::with_pause(scheduler.clone(), scheduler.task_scheduler(), scheduler)
}

fn require_db_url(url: Option<String>) -> String {
    url.unwrap_or_else(|| {
        eprintln!("error: DATABASE_URL must be set (or pass --database-url)");
        std::process::exit(1);
    })
}

async fn connect(url: &str) -> sqlx::PgPool {
    sqlx::PgPool::connect(url).await.unwrap_or_else(|e| {
        eprintln!("error: could not connect to database: {e}");
        std::process::exit(1);
    })
}
