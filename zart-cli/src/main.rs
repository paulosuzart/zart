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
            // TODO(M1): connect to Postgres, run migrations.
            eprintln!("zart migrate — not yet implemented (M1)");
            std::process::exit(1);
        }

        Commands::Schedule {
            execution_id,
            task_name,
            data,
        } => {
            // TODO(M2): parse data JSON, connect to Postgres, schedule task.
            let _execution_id = execution_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let _data: serde_json::Value = serde_json::from_str(&data).unwrap_or_else(|e| {
                eprintln!("Invalid JSON payload: {e}");
                std::process::exit(1);
            });
            eprintln!("zart schedule {task_name} — not yet implemented (M2)");
            std::process::exit(1);
        }

        Commands::Status { execution_id } => {
            // TODO(M2): connect to Postgres, fetch and display execution status.
            eprintln!("zart status {execution_id} — not yet implemented (M2)");
            std::process::exit(1);
        }

        Commands::Cancel { execution_id } => {
            // TODO(M4): connect to Postgres, cancel execution.
            eprintln!("zart cancel {execution_id} — not yet implemented (M4)");
            std::process::exit(1);
        }

        Commands::Wait {
            execution_id,
            timeout_secs: _,
        } => {
            // TODO(M4): poll until completion or timeout.
            eprintln!("zart wait {execution_id} — not yet implemented (M4)");
            std::process::exit(1);
        }
    }
}
