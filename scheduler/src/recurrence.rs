//! Recurrence configuration for recurring tasks.

use serde::{Deserialize, Serialize};

/// Describes how a task should recur after completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Recurrence {
    /// Execute on a cron schedule in the given timezone.
    ///
    /// # Example
    /// ```json
    /// { "type": "cron", "expression": "0 */5 * * * *", "timezone": "America/Sao_Paulo" }
    /// ```
    Cron {
        /// A cron expression (6-field format: sec min hour dom month dow).
        expression: String,
        /// IANA timezone name (e.g. `"America/Sao_Paulo"`, `"UTC"`).
        timezone: String,
    },

    /// Execute at a fixed delay after the previous run completes.
    ///
    /// # Example
    /// ```json
    /// { "type": "fixed_delay", "duration_ms": 300000 }
    /// ```
    FixedDelay {
        /// Milliseconds to wait after completion before the next run.
        duration_ms: u64,
    },
}
