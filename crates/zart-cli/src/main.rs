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
//! | `zart retry-step` | Admin | Retry a dead step |
//! | `zart restart` | Admin | Restart entire execution |
//! | `zart rerun` | Admin | Selective rerun of steps |
//! | `zart pause` | Admin | Create a pause rule |
//! | `zart resume` | Admin | Soft-delete pause rules |
//! | `zart pause-list` | Admin | List pause rules |
//!
//! # Configuration
//!
//! The CLI reads connection settings from environment variables or a config file:
//!
//! - `DATABASE_URL` — PostgreSQL connection string
//! - `ZART_CONFIG` — path to a TOML config file (optional)

use clap::{Parser, Subcommand};
use std::sync::Arc;
use zart::DurableScheduler;
use zart::admin::{PauseScope, RerunSpec};
use zart_scheduler::Scheduler as _;
use zart_scheduler::pause_storage::PauseRuleFilter;

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

    // ── Admin commands ────────────────────────────────────────────────────
    /// Retry a dead step within the current run.
    RetryStep {
        /// The execution ID.
        execution_id: String,

        /// Name of the step to retry.
        step_name: String,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// Restart an entire execution from scratch (preserves history).
    Restart {
        /// The execution ID.
        execution_id: String,

        /// Optional new JSON payload (keeps existing if omitted).
        #[arg(long)]
        payload: Option<String>,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// Selectively rerun a subset of steps while preserving others.
    Rerun {
        /// The execution ID.
        execution_id: String,

        /// Steps to force-rerun (comma-separated).
        #[arg(long, value_delimiter = ',')]
        rerun: Vec<String>,

        /// Steps to preserve (comma-separated).
        #[arg(long, value_delimiter = ',')]
        preserve: Vec<String>,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// List past runs for an execution.
    Runs {
        /// The execution ID.
        execution_id: String,
    },

    /// Create a pause rule.
    Pause {
        /// Target a specific execution ID.
        #[arg(long)]
        execution_id: Option<String>,

        /// Target all executions of a task name.
        #[arg(long)]
        task_name: Option<String>,

        /// Glob pattern for step names (e.g. 'send-*').
        #[arg(long)]
        step: Option<String>,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// Resume execution by soft-deleting pause rules.
    Resume {
        /// Target a specific execution ID.
        #[arg(long)]
        execution_id: Option<String>,

        /// Target all executions of a task name.
        #[arg(long)]
        task_name: Option<String>,

        /// Glob pattern for step names.
        #[arg(long)]
        step: Option<String>,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// List pause rules.
    PauseList {
        /// Include soft-deleted rules.
        #[arg(long)]
        include_deleted: bool,
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
            let scheduler = zart_scheduler::PostgresScheduler::new(pool);
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
            let scheduler = Arc::new(zart_scheduler::PostgresScheduler::new(pool));
            let durable = DurableScheduler::new(scheduler);

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
            let scheduler = Arc::new(zart_scheduler::PostgresScheduler::new(pool));
            let durable = DurableScheduler::new(scheduler);

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
            let scheduler = Arc::new(zart_scheduler::PostgresScheduler::new(pool));
            let durable = DurableScheduler::new(scheduler);

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
            let scheduler = Arc::new(zart_scheduler::PostgresScheduler::new(pool));
            let durable = DurableScheduler::new(scheduler);

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

        Commands::RetryStep {
            execution_id,
            step_name,
            triggered_by,
        } => {
            let durable = connect_pause_enabled(&cli.database_url).await;
            let run_id = get_run_id_or_exit(&durable, &execution_id).await;

            let task_id = durable
                .retry_step(&run_id, &step_name, triggered_by.as_deref())
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            println!("Step '{step_name}' retried (new task: {task_id}).");
        }

        Commands::Restart {
            execution_id,
            payload,
            triggered_by,
        } => {
            let durable = connect_pause_enabled(&cli.database_url).await;

            let new_payload = payload.map(|p| {
                serde_json::from_str(&p).unwrap_or_else(|e| {
                    eprintln!("error: invalid JSON payload: {e}");
                    std::process::exit(1);
                })
            });

            let new_run_id = durable
                .restart(&execution_id, new_payload, triggered_by.as_deref())
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            println!("Execution '{execution_id}' restarted (new run: {new_run_id}).");
        }

        Commands::Rerun {
            execution_id,
            rerun,
            preserve,
            triggered_by,
        } => {
            let durable = connect_pause_enabled(&cli.database_url).await;

            let spec = RerunSpec {
                force_rerun: rerun,
                preserve,
                triggered_by,
            };

            let result = durable
                .rerun_steps(&execution_id, spec)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            println!("New run number: {}", result.new_run_number);
            println!("Steps to rerun: {}", result.effective_rerun.join(", "));
        }

        Commands::Runs { execution_id } => {
            let durable = connect_pause_enabled(&cli.database_url).await;
            let runs = durable.list_runs(&execution_id).await.unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            });

            if runs.is_empty() {
                eprintln!("No runs found for execution '{execution_id}'.");
                std::process::exit(1);
            }

            println!("Runs for execution '{execution_id}':");
            for r in &runs {
                let marker = if r.status == zart_scheduler::ExecutionStatus::Completed {
                    "✓"
                } else {
                    ""
                };
                println!(
                    "  run:{}  status:{}  started:{}  trigger:{}",
                    r.run_index,
                    r.status,
                    r.started_at,
                    format!("{:?}", r.trigger).to_lowercase(),
                );
                if let Some(result) = &r.result {
                    println!("    result: {result}");
                }
                if !marker.is_empty() {
                    println!("    {marker}");
                }
            }
        }

        Commands::Pause {
            execution_id,
            task_name,
            step,
            triggered_by,
        } => {
            let durable = connect_pause_enabled(&cli.database_url).await;

            let rule = durable
                .pause(PauseScope {
                    execution_id,
                    task_name,
                    step_pattern: step,
                    triggered_by,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            println!("Created pause rule '{}' ({:?}).", rule.rule_id, rule.scope);
        }

        Commands::Resume {
            execution_id,
            task_name,
            step,
            triggered_by,
        } => {
            let durable = connect_pause_enabled(&cli.database_url).await;

            let result = durable
                .resume(PauseScope {
                    execution_id,
                    task_name,
                    step_pattern: step,
                    triggered_by,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            println!(
                "Resumed: {} pause rule(s) soft-deleted.",
                result.rules_deleted
            );
        }

        Commands::PauseList { include_deleted } => {
            let durable = connect_pause_enabled(&cli.database_url).await;

            let rules = durable
                .list_pause_rules(Some(PauseRuleFilter {
                    include_deleted,
                    ..Default::default()
                }))
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            if rules.is_empty() {
                println!("No pause rules found.");
            } else {
                for r in &rules {
                    let deleted_marker = if r.deleted_at.is_some() {
                        " [DELETED]"
                    } else {
                        ""
                    };
                    println!(
                        "{} {:?} (created: {}){}",
                        r.rule_id, r.scope, r.created_at, deleted_marker
                    );
                }
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

/// Connect with pause storage enabled (PostgresScheduler implements PauseStorage).
async fn connect_pause_enabled(db_url: &Option<String>) -> DurableScheduler {
    let url = require_db_url(db_url.clone());
    let pool = connect(&url).await;
    let scheduler = Arc::new(zart_scheduler::PostgresScheduler::new(pool));
    DurableScheduler::with_pause(scheduler.clone(), scheduler)
}

/// Get the current run_id for an execution or exit with an error.
async fn get_run_id_or_exit(durable: &DurableScheduler, execution_id: &str) -> String {
    durable
        .get_current_run_id(execution_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            eprintln!("error: no run found for execution '{execution_id}'");
            std::process::exit(1);
        })
}
