//! Zart CLI — command-line interface for the Zart durable execution framework.
//!
//! # Commands
//!
//! | Command | Description |
//! |---------|-------------|
//! | `zart migrate` | Apply database migrations |
//! | `zart schedule` | Schedule a task immediately |
//! | `zart status` | Inspect an execution's status |
//! | `zart cancel` | Cancel a running execution |
//! | `zart wait` | Block until an execution completes |
//! | `zart retry-step` | Retry a dead step |
//! | `zart restart` | Restart entire execution |
//! | `zart rerun` | Selective rerun of steps |
//! | `zart pause` | Create a pause rule |
//! | `zart resume` | Soft-delete pause rules (by scope) |
//! | `zart delete-pause-rule` | Soft-delete a pause rule by ID |
//! | `zart pause-list` | List pause rules |
//! | `zart detail` | Full execution detail with steps and attempts |
//!
//! # Configuration
//!
//! The CLI reads connection settings from environment variables or a config file:
//!
//! - `DATABASE_URL` — PostgreSQL connection string
//! - `ZART_CONFIG` — path to a TOML config file (optional)

mod cmd;
mod db;
mod fmt;

use clap::Parser;

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
    command: cmd::Commands,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    cmd::run(cli.command, cli.database_url).await;
}
