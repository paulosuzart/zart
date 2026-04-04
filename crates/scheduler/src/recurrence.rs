//! Recurrence configuration for recurring tasks.

use chrono::{DateTime, Utc};
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

impl Recurrence {
    /// Compute the next execution time strictly after `now`.
    ///
    /// Returns `None` if the cron expression is invalid, the timezone cannot be
    /// parsed, or the schedule produces no future occurrences.
    pub fn next_after(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Recurrence::Cron {
                expression,
                timezone,
            } => {
                use std::str::FromStr;
                let schedule = cron::Schedule::from_str(expression).ok()?;
                let tz: chrono_tz::Tz = timezone.parse().ok()?;
                let now_in_tz = now.with_timezone(&tz);
                schedule
                    .after(&now_in_tz)
                    .next()
                    .map(|dt| dt.with_timezone(&Utc))
            }
            Recurrence::FixedDelay { duration_ms } => {
                let delta = chrono::Duration::milliseconds(*duration_ms as i64);
                Some(now + delta)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn fixed_delay_next_after_adds_duration() {
        let r = Recurrence::FixedDelay {
            duration_ms: 60_000,
        };
        let now = Utc::now();
        let next = r.next_after(now).unwrap();
        let diff = (next - now).num_milliseconds();
        assert!((59_900..=60_100).contains(&diff), "diff={diff}");
    }

    #[test]
    fn cron_next_after_is_in_the_future() {
        // Every minute
        let r = Recurrence::Cron {
            expression: "0 * * * * *".to_string(),
            timezone: "UTC".to_string(),
        };
        let now = Utc::now();
        let next = r.next_after(now).unwrap();
        assert!(next > now, "next={next} should be after now={now}");
    }

    #[test]
    fn cron_invalid_expression_returns_none() {
        let r = Recurrence::Cron {
            expression: "not-a-cron".to_string(),
            timezone: "UTC".to_string(),
        };
        assert!(r.next_after(Utc::now()).is_none());
    }

    #[test]
    fn cron_invalid_timezone_returns_none() {
        let r = Recurrence::Cron {
            expression: "0 * * * * *".to_string(),
            timezone: "Not/A_Timezone".to_string(),
        };
        assert!(r.next_after(Utc::now()).is_none());
    }
}
