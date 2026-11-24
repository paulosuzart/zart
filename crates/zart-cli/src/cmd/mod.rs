mod admin_exec;
mod admin_pause;
mod exec;

use clap::Subcommand;

use crate::db;

#[derive(Subcommand)]
pub enum Commands {
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

    /// Delete (soft-delete) a pause rule by its ID.
    DeletePauseRule {
        /// The pause rule ID to soft-delete.
        rule_id: String,

        /// Optional operator identifier for audit.
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// Show full execution detail: runs, steps, and attempt history.
    Detail {
        /// The execution ID to inspect.
        execution_id: String,

        /// Show steps from this specific run (defaults to current run).
        #[arg(long)]
        run_id: Option<String>,
    },
}

pub async fn run(command: Commands, database_url: Option<String>) {
    match command {
        Commands::Migrate => {
            let pool = db::pool(database_url).await;
            exec::migrate(pool).await;
        }

        Commands::Schedule {
            execution_id,
            task_name,
            data,
        } => {
            let durable = db::simple(database_url).await;
            exec::schedule(durable, execution_id, task_name, data).await;
        }

        Commands::Status { execution_id } => {
            let durable = db::simple(database_url).await;
            exec::status(durable, execution_id).await;
        }

        Commands::Cancel { execution_id } => {
            let durable = db::simple(database_url).await;
            exec::cancel(durable, execution_id).await;
        }

        Commands::Wait {
            execution_id,
            timeout_secs,
        } => {
            let durable = db::simple(database_url).await;
            exec::wait(durable, execution_id, timeout_secs).await;
        }

        Commands::RetryStep {
            execution_id,
            step_name,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_exec::retry_step(durable, execution_id, step_name, triggered_by).await;
        }

        Commands::Restart {
            execution_id,
            payload,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_exec::restart(durable, execution_id, payload, triggered_by).await;
        }

        Commands::Rerun {
            execution_id,
            rerun,
            preserve,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_exec::rerun(durable, execution_id, rerun, preserve, triggered_by).await;
        }

        Commands::Runs { execution_id } => {
            let durable = db::admin(database_url).await;
            admin_exec::runs(durable, execution_id).await;
        }

        Commands::Detail {
            execution_id,
            run_id,
        } => {
            let durable = db::admin(database_url).await;
            admin_exec::detail(durable, execution_id, run_id).await;
        }

        Commands::Pause {
            execution_id,
            task_name,
            step,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_pause::pause(durable, execution_id, task_name, step, triggered_by).await;
        }

        Commands::Resume {
            execution_id,
            task_name,
            step,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_pause::resume(durable, execution_id, task_name, step, triggered_by).await;
        }

        Commands::PauseList { include_deleted } => {
            let durable = db::admin(database_url).await;
            admin_pause::pause_list(durable, include_deleted).await;
        }

        Commands::DeletePauseRule {
            rule_id,
            triggered_by,
        } => {
            let durable = db::admin(database_url).await;
            admin_pause::delete_pause_rule(durable, rule_id, triggered_by).await;
        }
    }
}
