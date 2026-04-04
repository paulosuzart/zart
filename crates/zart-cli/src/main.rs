//! Zart CLI — command-line interface for the Zart durable execution framework.
//!
//! # Commands
//!
//! | Command | Milestone | Description |
//! |---------|-----------|-------------|
//! | `zart migrate` | M1 | Apply database migrations |
//! | `zart schedule` | M2 | Schedule a task immediately |
//! | `zart status` | M2 | Inspect an execution's status |
//! | `zart cancel` | M4 | Cancel a running execution |
//! | `zart wait` | M4 | Block until an execution completes |
//!
//! # Configuration
//!
//! The CLI reads connection settings from environment variables or a config file:
//!
//! - `DATABASE_URL` — PostgreSQL connection string
//! - `ZART_CONFIG` — path to a TOML config file (optional)

use clap::{Parser, Subcommand};
use scheduler::Scheduler as _;
use std::sync::Arc;
use zart::{DurableScheduler, TaskRegistry};

/// Zart — Durable Execution Framework CLI
#[derive(Parser)]
#[command(
    name = "zart",
    version,
    about = "Manage Zart durable executions from the command line",
    long_about = None,
)]
struct Cli {
    /// PostgreSQL connection URL (overrides DATABASE_URL env var).
    #[arg(long, env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    /// Enable verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply database migrations (creates `zart_tasks` and `zart_executions` tables).
    ///
    /// Connects to the configured PostgreSQL instance and runs all pending migrations.
    Migrate,

    /// Schedule a task for immediate execution.
    ///
    /// The task must be registered in the worker that will pick it up.
    Schedule {
        /// Unique execution ID (idempotency key). Auto-generated if omitted.
        #[arg(long)]
        execution_id: Option<String>,

        /// Registered task name.
        #[arg(long)]
        task_name: String,

        /// JSON payload to pass to the task handler.
        #[arg(long, default_value = "{}")]
        data: String,
    },

    /// Show the current status of a durable execution.
    Status {
        /// The execution ID to inspect.
        execution_id: String,
    },

    /// Cancel a running durable execution.
    Cancel {
        /// The execution ID to cancel.
        execution_id: String,
    },

    /// Wait (blocking) until a durable execution completes or fails.
    Wait {
        /// The execution ID to wait for.
        execution_id: String,

        /// Maximum seconds to wait before giving up.
        #[arg(long, default_value = "30")]
        timeout_secs: u64,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialise logging.
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    match cli.command {
        Commands::Migrate => {
            let url = require_db_url(cli.database_url);
            let pool = connect(&url).await;
            let scheduler = scheduler::PostgresScheduler::new(pool);
            scheduler.run_migrations().await.unwrap_or_else(|e| {
                eprintln!("error: migrations failed: {e}");
                std::process::exit(1);
            });
            println!("Migrations applied successfully.");
        }

        Commands::Schedule {
            execution_id,
            task_name,
            data,
        } => {
            let payload: serde_json::Value = serde_json::from_str(&data).unwrap_or_else(|e| {
                eprintln!("error: invalid JSON payload: {e}");
                std::process::exit(1);
            });

            let execution_id = execution_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let url = require_db_url(cli.database_url);
            let pool = connect(&url).await;
            let scheduler = Arc::new(scheduler::PostgresScheduler::new(pool));
            let registry: Arc<TaskRegistry<scheduler::PostgresScheduler>> =
                Arc::new(TaskRegistry::new());
            let durable = DurableScheduler::new(scheduler, registry);

            durable
                .start(&execution_id, &task_name, payload)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: failed to schedule execution: {e}");
                    std::process::exit(1);
                });

            println!("Scheduled execution '{execution_id}' for task '{task_name}'.");
        }

        Commands::Status { execution_id } => {
            let url = require_db_url(cli.database_url);
            let pool = connect(&url).await;
            let scheduler = Arc::new(scheduler::PostgresScheduler::new(pool));
            let registry: Arc<TaskRegistry<scheduler::PostgresScheduler>> =
                Arc::new(TaskRegistry::new());
            let durable = DurableScheduler::new(scheduler, registry);

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

        Commands::Cancel { execution_id } => {
            let url = require_db_url(cli.database_url);
            let pool = connect(&url).await;
            let scheduler = Arc::new(scheduler::PostgresScheduler::new(pool));
            let registry: Arc<TaskRegistry<scheduler::PostgresScheduler>> =
                Arc::new(TaskRegistry::new());
            let durable = DurableScheduler::new(scheduler, registry);

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

        Commands::Wait {
            execution_id,
            timeout_secs,
        } => {
            let url = require_db_url(cli.database_url);
            let pool = connect(&url).await;
            let scheduler = Arc::new(scheduler::PostgresScheduler::new(pool));
            let registry: Arc<TaskRegistry<scheduler::PostgresScheduler>> =
                Arc::new(TaskRegistry::new());
            let durable = DurableScheduler::new(scheduler, registry);

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
    }
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
