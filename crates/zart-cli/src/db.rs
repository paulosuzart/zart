use zart::{DurableScheduler, PgBackend};

/// Return a raw connection pool (used by `migrate`).
pub async fn pool(db_url: Option<String>) -> sqlx::PgPool {
    let url = require_db_url(db_url);
    connect(&url).await
}

/// Connect and return a DurableScheduler backed by PgBackend.
pub async fn simple(db_url: Option<String>) -> DurableScheduler {
    let url = require_db_url(db_url);
    let pool = connect(&url).await;
    let pg = PgBackend::new(pool);
    DurableScheduler::from_backend(&pg)
}

/// Connect and return a DurableScheduler backed by PgBackend (pause always enabled).
pub async fn admin(db_url: Option<String>) -> DurableScheduler {
    simple(db_url).await
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
