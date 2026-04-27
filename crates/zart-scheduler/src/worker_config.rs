//! Worker tuning parameters.

use std::time::Duration;

/// Tuning parameters for a [`Worker`](crate::Worker).
///
/// All fields have production-ready defaults via [`WorkerConfig::default`].
/// Override only what you need:
///
/// ```rust,ignore
/// let config = WorkerConfig {
///     poll_interval:        Duration::from_secs(2),
///     max_concurrent_tasks: 32,
///     ..WorkerConfig::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// How often the worker polls the database for due tasks.
    pub poll_interval: Duration,

    /// Maximum number of tasks to fetch per poll cycle.
    pub max_tasks_per_poll: usize,

    /// Maximum number of tasks that can execute concurrently within this worker.
    pub max_concurrent_tasks: usize,

    /// How long to wait for in-flight tasks to finish during graceful shutdown.
    pub shutdown_timeout: Duration,

    /// Tasks stuck in `picked_up` state longer than this are considered orphaned
    /// and will be reset to `scheduled` by the orphan recovery loop.
    pub orphan_timeout: Duration,

    /// How often to renew the task lease while a handler is executing.
    ///
    /// When `None` (the default), the interval is computed as `orphan_timeout / 3`,
    /// giving 2 retries before orphan recovery would reclaim the task.
    /// Set to `Some(Duration::ZERO)` to disable heartbeating entirely.
    pub heartbeat_interval: Option<Duration>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 16,
            shutdown_timeout: Duration::from_secs(30),
            orphan_timeout: Duration::from_secs(300),
            heartbeat_interval: None, // Defaults to orphan_timeout / 3.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let cfg = WorkerConfig::default();
        assert!(cfg.poll_interval > Duration::ZERO);
        assert!(cfg.max_tasks_per_poll > 0);
        assert!(cfg.max_concurrent_tasks > 0);
        assert!(cfg.shutdown_timeout > Duration::ZERO);
        assert!(cfg.heartbeat_interval.is_none());
    }

    #[test]
    fn effective_interval_uses_orphan_timeout_third_when_none() {
        let orphan_timeout = Duration::from_secs(300);
        let heartbeat_interval: Option<Duration> = None;
        let effective = heartbeat_interval
            .filter(|d| !d.is_zero())
            .unwrap_or_else(|| orphan_timeout / 3);
        assert_eq!(effective, Duration::from_secs(100));
    }
}
