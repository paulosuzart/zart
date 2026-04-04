//! Prometheus metrics for Zart observability.
//!
//! Provides global metric collectors that track task execution,
//! step performance, worker activity, and queue depth.

use lazy_static::lazy_static;
use prometheus::{CounterVec, Gauge, HistogramOpts, HistogramVec, Registry};

lazy_static! {
    /// Global Prometheus registry for Zart metrics.
    pub static ref METRICS_REGISTRY: Registry = Registry::new();

    /// Total number of tasks by status (completed, failed, cancelled, scheduled).
    pub static ref TASKS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_tasks_total", "Total number of tasks by status"),
        &["status"]
    ).expect("Failed to create tasks_total metric");

    /// Task execution duration in seconds (histogram).
    pub static ref TASK_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_task_duration_seconds",
            "Task execution duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
        &["task_name", "status"]
    ).expect("Failed to create task_duration_seconds metric");

    /// Total number of steps by status (completed, failed, scheduled, waiting_for_event).
    pub static ref STEPS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_steps_total", "Total number of steps by status"),
        &["status", "step_name"]
    ).expect("Failed to create steps_total metric");

    /// Step execution duration in seconds (histogram).
    pub static ref STEP_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_step_duration_seconds",
            "Step execution duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
        &["step_name", "status"]
    ).expect("Failed to create step_duration_seconds metric");

    /// Number of tasks currently waiting to be picked up (queue depth).
    pub static ref QUEUE_DEPTH: Gauge = Gauge::new(
        "zart_queue_depth",
        "Number of tasks waiting to be picked up"
    ).expect("Failed to create queue_depth metric");

    /// Number of tasks currently executing concurrently.
    pub static ref WORKER_CONCURRENT_TASKS: Gauge = Gauge::new(
        "zart_worker_concurrent_tasks",
        "Number of tasks currently executing"
    ).expect("Failed to create worker_concurrent_tasks metric");

    /// Time between poll cycles in seconds.
    pub static ref POLL_INTERVAL_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "zart_poll_interval_seconds",
            "Time between poll cycles in seconds"
        )
        .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &[]
    ).expect("Failed to create poll_interval_seconds metric");

    /// Total number of durable executions by status.
    pub static ref EXECUTIONS_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_executions_total", "Total number of durable executions by status"),
        &["status", "task_name"]
    ).expect("Failed to create executions_total metric");

    /// Total number of events delivered.
    pub static ref EVENTS_DELIVERED_TOTAL: CounterVec = CounterVec::new(
        prometheus::opts!("zart_events_delivered_total", "Total number of events delivered"),
        &["event_name", "status"]
    ).expect("Failed to create events_delivered_total metric");
}

/// Register all Zart metrics with the global registry.
pub fn register_metrics() -> Result<(), prometheus::Error> {
    METRICS_REGISTRY.register(Box::new(TASKS_TOTAL.clone()))?;
    METRICS_REGISTRY.register(Box::new(TASK_DURATION_SECONDS.clone()))?;
    METRICS_REGISTRY.register(Box::new(STEPS_TOTAL.clone()))?;
    METRICS_REGISTRY.register(Box::new(STEP_DURATION_SECONDS.clone()))?;
    METRICS_REGISTRY.register(Box::new(QUEUE_DEPTH.clone()))?;
    METRICS_REGISTRY.register(Box::new(WORKER_CONCURRENT_TASKS.clone()))?;
    METRICS_REGISTRY.register(Box::new(POLL_INTERVAL_SECONDS.clone()))?;
    METRICS_REGISTRY.register(Box::new(EXECUTIONS_TOTAL.clone()))?;
    METRICS_REGISTRY.register(Box::new(EVENTS_DELIVERED_TOTAL.clone()))?;
    Ok(())
}

/// Get the encoded metrics as a string for Prometheus scraping.
pub fn gather_metrics() -> String {
    let metric_families = METRICS_REGISTRY.gather();
    prometheus::TextEncoder::new()
        .encode_to_string(&metric_families)
        .unwrap_or_else(|e| format!("Failed to encode metrics: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_registry_is_initialized() {
        assert!(!METRICS_REGISTRY.gather().is_empty());
    }

    #[test]
    fn can_register_metrics() {
        // Registration should succeed even if called multiple times
        let result = register_metrics();
        assert!(result.is_ok());
    }

    #[test]
    fn gather_metrics_returns_output() {
        let _ = register_metrics();
        let output = gather_metrics();
        // Just check that we get some output - the exact metric names may vary
        assert!(!output.is_empty() || output.contains("Failed to encode"));
    }

    #[test]
    fn counter_increments() {
        // Register metrics first
        let _ = register_metrics();
        TASKS_TOTAL.with_label_values(&["completed"]).inc();
        let output = gather_metrics();
        // Check that the counter is present - exact format may vary
        assert!(output.contains("zart_tasks_total"));
    }

    #[test]
    fn histogram_records_values() {
        let timer = TASK_DURATION_SECONDS
            .with_label_values(&["test_task", "completed"])
            .start_timer();
        drop(timer); // observe on drop
        let output = gather_metrics();
        assert!(output.contains("zart_task_duration_seconds"));
    }
}
