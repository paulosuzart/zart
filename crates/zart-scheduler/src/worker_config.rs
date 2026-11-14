//! Worker tuning parameters.

use std::time::Duration;

/// Tuning parameters for a [`Worker`](crate::Worker).
///
/// Zart uses a **Notify-Hint & Adaptive Drain** dispatch model:
/// *"Notifications are hints, never truth."*
///
/// - **Sub-100ms wakeup**: When `enable_listen_notify` is `true` (the default),
///   PostgreSQL `LISTEN/NOTIFY` and in-process `local_wakeup` hints wake idle
///   workers immediately when a due task is scheduled or resumed.
/// - **Fallback reconciliation**: `poll_interval` acts as a safety-net poll for
///   future-scheduled tasks becoming due or missed notifications, and can safely
///   be set to `30s`–`60s` in production.
/// - **Adaptive drain & backpressure**: Each poll clamps its fetch limit to
///   `min(max_tasks_per_poll, available_permits)` against
///   `max_concurrent_tasks`. When a full batch is returned, the worker drains
///   the next batch immediately without sleeping `poll_interval`; when capacity
///   is full, it suspends until an in-flight task finishes.
///
/// All fields have production-ready defaults via [`WorkerConfig::default`].
/// Override only what you need:
///
/// ```rust
/// use std::time::Duration;
/// use zart_scheduler::WorkerConfig;
///
/// let config = WorkerConfig {
///     poll_interval:        Duration::from_secs(30),
///     max_concurrent_tasks: 32,
///     max_tasks_per_poll:   10,
///     enable_listen_notify: true,
///     ..WorkerConfig::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// How often the worker polls the database for due tasks.
    ///
    /// When `enable_listen_notify` is `true` (default), this acts as the
    /// fallback reconciliation poll interval (can safely be set to `30s`–`60s`
    /// in production without hurting task wakeup latency).
    pub poll_interval: Duration,

    /// Maximum number of tasks to fetch per poll cycle.
    ///
    /// The actual fetch limit is clamped to `min(max_tasks_per_poll, available_permits)`
    /// so the worker never over-fetches beyond `max_concurrent_tasks`.
    /// When a poll returns a full batch, the worker enters adaptive drain mode
    /// and immediately polls again without waiting for `poll_interval`.
    pub max_tasks_per_poll: usize,

    /// Maximum number of tasks that can execute concurrently within this worker.
    ///
    /// Enforced via a semaphore. When all permits are in use, the worker pauses
    /// polling (backpressure) until an in-flight task completes and releases a permit.
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

    /// Enable PostgreSQL `LISTEN / NOTIFY` wakeup hints when supported by the
    /// scheduler backend. Defaults to `true`.
    ///
    /// Even when `false`, in-process `local_wakeup` hints and the `poll_interval`
    /// fallback loop remain active.
    pub enable_listen_notify: bool,
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
            enable_listen_notify: true,
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
        assert!(cfg.enable_listen_notify);
    }

    #[test]
    fn struct_update_syntax_preserves_wakeup_defaults() {
        let cfg = WorkerConfig {
            max_concurrent_tasks: 32,
            ..WorkerConfig::default()
        };
        assert_eq!(cfg.max_concurrent_tasks, 32);
        assert!(cfg.enable_listen_notify);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
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
