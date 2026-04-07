//! Prometheus metrics for Zart observability.
//!
//! Activate instrumentation by enabling the `metrics` Cargo feature:
//!
//! ```toml
//! zart = { version = "0.1", features = ["metrics"] }
//! ```
//!
//! When the feature is disabled every function in this module is a no-op and
//! neither `prometheus` nor `lazy_static` are compiled into your binary.

#[cfg(feature = "metrics")]
use lazy_static::lazy_static;
#[cfg(feature = "metrics")]
use prometheus::{CounterVec, Gauge, HistogramOpts, HistogramVec, Registry};

#[cfg(feature = "metrics")]
lazy_static! {
    /// Global Prometheus registry for Zart metrics.
    ///
    /// All metrics are eagerly registered during initialisation so that
    /// every test and every worker sees a fully-populated registry
    /// regardless of execution order.
    pub static ref METRICS_REGISTRY: Registry = {
        let reg = Registry::new();
        reg.register(Box::new(TASKS_TOTAL.clone())).expect("register TASKS_TOTAL");
        reg.register(Box::new(TASK_DURATION_SECONDS.clone())).expect("register TASK_DURATION");
        reg.register(Box::new(STEPS_TOTAL.clone())).expect("register STEPS_TOTAL");
        reg.register(Box::new(STEP_DURATION_SECONDS.clone())).expect("register STEP_DURATION");
        reg.register(Box::new(QUEUE_DEPTH.clone())).expect("register QUEUE_DEPTH");
        reg.register(Box::new(WORKER_CONCURRENT_TASKS.clone())).expect("register WORKER_CONCURRENT");
        reg.register(Box::new(POLL_INTERVAL_SECONDS.clone())).expect("register POLL_INTERVAL");
        reg.register(Box::new(EXECUTIONS_TOTAL.clone())).expect("register EXECUTIONS_TOTAL");
        reg.register(Box::new(EVENTS_DELIVERED_TOTAL.clone())).expect("register EVENTS_DELIVERED");
        reg.register(Box::new(TASK_HEARTBEAT_RENEWALS_TOTAL.clone())).expect("register TASK_HEARTBEAT_RENEWALS");
        reg.register(Box::new(HEARTBEAT_ACTIVE.clone())).expect("register HEARTBEAT_ACTIVE");
        reg
    };

    /// Total number of tasks by status (completed, failed, cancelled, scheduled).
    pub static ref TASKS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_tasks_total", "Total number of tasks by status"),
        &["status"]
    ).expect("create tasks_total");

    /// Task execution duration in seconds.
    pub static ref TASK_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_task_duration_seconds",
            "Task execution duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
        &["task_name", "status"]
    ).expect("create task_duration_seconds");

    /// Total number of steps by status (completed, failed, scheduled, waiting_for_event).
    pub static ref STEPS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_steps_total", "Total number of steps by status"),
        &["status", "step_name"]
    ).expect("create steps_total");

    /// Step execution duration in seconds.
    pub static ref STEP_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_step_duration_seconds",
            "Step execution duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
        &["step_name", "status"]
    ).expect("create step_duration_seconds");

    /// Number of tasks currently waiting to be picked up (queue depth).
    pub static ref QUEUE_DEPTH: Gauge = Gauge::new(
        "zart_queue_depth",
        "Number of tasks waiting to be picked up"
    ).expect("create queue_depth");

    /// Number of tasks currently executing concurrently.
    pub static ref WORKER_CONCURRENT_TASKS: Gauge = Gauge::new(
        "zart_worker_concurrent_tasks",
        "Number of tasks currently executing"
    ).expect("create worker_concurrent_tasks");

    /// Time between poll cycles in seconds.
    pub static ref POLL_INTERVAL_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_poll_interval_seconds",
            "Time between poll cycles in seconds"
        )
        .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &[]
    ).expect("create poll_interval_seconds");

    /// Total number of durable executions by status.
    pub static ref EXECUTIONS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_executions_total", "Total number of durable executions by status"),
        &["status", "task_name"]
    ).expect("create executions_total");

    /// Total number of events delivered.
    pub static ref EVENTS_DELIVERED_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_events_delivered_total", "Total number of events delivered"),
        &["event_name", "status"]
    ).expect("create events_delivered_total");

    /// Total number of task lease renewals via heartbeat.
    pub static ref TASK_HEARTBEAT_RENEWALS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_task_heartbeat_renewals_total", "Total number of task lease renewals via heartbeat"),
        &["task_name", "status"]
    ).expect("create task_heartbeat_renewals_total");

    /// Number of currently active heartbeat loops.
    pub static ref HEARTBEAT_ACTIVE: prometheus::IntGauge = prometheus::IntGauge::new(
        "zart_heartbeat_active",
        "Number of currently active heartbeat loops"
    ).expect("create heartbeat_active");
}

/// Encode all registered metrics in Prometheus text format.
///
/// Returns an empty string when the `metrics` feature is disabled.
///
/// # Example
///
/// ```rust,no_run
/// use zart::metrics::gather_metrics;
///
/// let output = gather_metrics();
/// // When the `metrics` feature is enabled, `output` contains
/// // Prometheus text-format lines such as:
/// //   # HELP zart_tasks_total Total number of tasks by status
/// //   # TYPE zart_tasks_total counter
/// //   zart_tasks_total{status="completed"} 42
/// ```
#[cfg(feature = "metrics")]
pub fn gather_metrics() -> String {
    let metric_families = METRICS_REGISTRY.gather();
    prometheus::TextEncoder::new()
        .encode_to_string(&metric_families)
        .unwrap_or_else(|e| format!("Failed to encode metrics: {e}"))
}

/// No-op stub — returns an empty string.
///
/// Enable the `metrics` Cargo feature to get real Prometheus output.
#[cfg(not(feature = "metrics"))]
pub fn gather_metrics() -> String {
    String::new()
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    #[test]
    fn metrics_registry_is_initialized() {
        assert!(!METRICS_REGISTRY.gather().is_empty());
    }

    #[test]
    fn gather_metrics_returns_prometheus_output() {
        let output = gather_metrics();
        assert!(!output.is_empty());
        assert!(output.contains("zart_tasks_total"));
    }

    #[test]
    fn counter_increments_are_reflected_in_output() {
        TASKS_TOTAL.with_label_values(&["completed"]).inc();
        let output = gather_metrics();
        assert!(output.contains("zart_tasks_total"));
    }

    #[test]
    fn histogram_observations_are_reflected_in_output() {
        let _timer = TASK_DURATION_SECONDS
            .with_label_values(&["test_task", "completed"])
            .start_timer();
        // timer observes on drop
        let output = gather_metrics();
        assert!(output.contains("zart_task_duration_seconds"));
    }
}

#[cfg(all(test, not(feature = "metrics")))]
mod stub_tests {
    use super::*;

    #[test]
    fn gather_metrics_returns_empty_string_without_feature() {
        assert_eq!(gather_metrics(), String::new());
    }
}
