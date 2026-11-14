//! Generic scheduler worker — polls the task queue and dispatches to registered handlers.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};

use crate::ops::ExecutionOps;
use crate::registry::TaskRegistry;
use crate::store::{TaskScheduler, WakeupOptions};
use crate::task::{CompletionHandler, TaskInstance};
use crate::types::{FetchedTask, ScheduleAtParams};
use crate::worker_config::WorkerConfig;

/// Polls the task queue and dispatches tasks to registered [`ScheduledTask`](crate::ScheduledTask) handlers.
///
/// The worker is generic: it knows nothing about durable executions, steps, or
/// runs. All application-specific logic lives in the handlers registered in
/// the [`TaskRegistry`].
///
/// # Dispatch model
///
/// For each fetched task the worker:
/// 1. Calls the registered handler's `execute(instance)` — no transaction is opened.
/// 2. The handler returns a [`CompletionHandler`] that encapsulates the outcome.
/// 3. The worker constructs [`ExecutionOps`] and delegates to
///    `completion.complete(ops, recurrence, execution_time)`.
/// 4. On error (from `execute` or `complete`), calls `mark_failed`.
///
/// The worker never opens a transaction; [`ExecutionOps`] opens them lazily
/// when needed, and [`CompletionHandler`] implementations may provide their
/// own transaction via [`crate::completion::WithTransaction`].
///
/// # Heartbeating
///
/// While a handler runs, a background task renews the row's lease at
/// `orphan_timeout / 3` intervals so the row is not reclaimed by orphan
/// recovery. Configurable via [`WorkerConfig::heartbeat_interval`].
pub struct Worker {
    scheduler: Arc<dyn TaskScheduler>,
    registry: Arc<TaskRegistry>,
    config: WorkerConfig,
    shutdown: Arc<Notify>,
    cancellation: CancellationToken,
    /// Recurring tasks to seed into the DB idempotently at the start of `run()`.
    pending_recurring: Vec<ScheduleAtParams>,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        scheduler: Arc<dyn TaskScheduler>,
        registry: Arc<TaskRegistry>,
        config: WorkerConfig,
        pending_recurring: Vec<ScheduleAtParams>,
    ) -> Self {
        Self {
            scheduler,
            registry,
            config,
            shutdown: Arc::new(Notify::new()),
            cancellation: CancellationToken::new(),
            pending_recurring,
        }
    }

    /// Create a new worker with a shared cancellation token.
    ///
    /// This allows multiple workers to be shut down together
    /// when a common `CancellationToken` is cancelled.
    pub fn with_cancellation(
        scheduler: Arc<dyn TaskScheduler>,
        registry: Arc<TaskRegistry>,
        config: WorkerConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            scheduler,
            registry,
            config,
            shutdown: Arc::new(Notify::new()),
            cancellation,
            pending_recurring: Vec::new(),
        }
    }

    /// Start the adaptive polling loop.
    ///
    /// Runs until [`stop`](Self::stop) is called or the cancellation token is
    /// triggered. Drains full batches without sleeping, waits on wakeup
    /// notifications or `poll_interval` when idle, and runs orphan recovery at
    /// `max(orphan_timeout / 2, 10s)` intervals.
    #[instrument(name = "scheduler_worker.run", skip(self))]
    pub async fn run(&self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            max_tasks = self.config.max_tasks_per_poll,
            concurrency = self.config.max_concurrent_tasks,
            orphan_timeout_secs = self.config.orphan_timeout.as_secs(),
            enable_listen_notify = self.config.enable_listen_notify,
            "Scheduler worker starting"
        );

        // Seed recurring durable tasks idempotently before entering the poll loop.
        for params in &self.pending_recurring {
            if let Err(e) = self.scheduler.schedule_at(params.clone()).await {
                error!(error = %e, task_id = %params.task_id, "Failed to seed recurring task");
            }
        }

        let wakeup = self
            .scheduler
            .subscribe_wakeups(
                WakeupOptions {
                    enable_listen_notify: self.config.enable_listen_notify,
                    subscribed_queues: Vec::new(),
                },
                self.cancellation.clone(),
            )
            .await;

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let permit_returned = Arc::new(Notify::new());
        let orphan_recovery_interval =
            (self.config.orphan_timeout / 2).max(Duration::from_secs(10));
        let mut last_orphan_recovery = tokio::time::Instant::now();

        loop {
            if self.cancellation.is_cancelled() {
                info!("Scheduler worker cancellation token triggered, exiting poll loop");
                break;
            }

            maybe_recover_orphans(
                &*self.scheduler,
                self.config.orphan_timeout,
                orphan_recovery_interval,
                &mut last_orphan_recovery,
            )
            .await;

            let Some(available) = wait_for_capacity(
                &semaphore,
                &permit_returned,
                &self.shutdown,
                &self.cancellation,
            )
            .await
            else {
                break;
            };

            let batch_limit = self.config.max_tasks_per_poll.min(available);
            let tasks = match self
                .scheduler
                .poll_due(chrono::Utc::now(), batch_limit)
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    error!(error = %e, "Failed to poll for due tasks");
                    if !wait_idle(
                        None,
                        self.config.poll_interval,
                        &self.shutdown,
                        &self.cancellation,
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }
            };

            let fetched_count = tasks.len();
            if fetched_count > 0 {
                info!(count = fetched_count, "Fetched tasks for dispatch");
            }

            for task in tasks {
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(
                            task_id = %task.task_id,
                            error = %e,
                            "No concurrency permit available for fetched task; skipping remaining tasks in batch"
                        );
                        break;
                    }
                };

                let guard = PermitGuard {
                    permit: Some(permit),
                    permit_returned: permit_returned.clone(),
                };
                let scheduler = self.scheduler.clone();
                let registry = self.registry.clone();
                let heartbeat_interval = self.config.heartbeat_interval;
                let orphan_timeout = self.config.orphan_timeout;

                tokio::spawn(
                    async move {
                        let _guard = guard;
                        dispatch_task(
                            scheduler,
                            registry,
                            task,
                            heartbeat_interval,
                            orphan_timeout,
                        )
                        .await;
                    }
                    .in_current_span(),
                );
            }

            if fetched_count == batch_limit {
                if semaphore.available_permits() > 0 {
                    tokio::task::yield_now().await;
                }
                continue;
            }

            if !wait_idle(
                wakeup.as_deref(),
                self.config.poll_interval,
                &self.shutdown,
                &self.cancellation,
            )
            .await
            {
                break;
            }
        }
    }

    /// Signal the worker to stop after the current poll cycle completes.
    pub fn stop(&self) {
        info!("Scheduler worker stop requested");
        self.shutdown.notify_one();
        self.cancellation.cancel();
    }

    /// Get the cancellation token for this worker.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

/// Holds a concurrency permit for a dispatched task and notifies the worker
/// poll loop as soon as the permit is released (even on early return or panic).
struct PermitGuard {
    permit: Option<OwnedSemaphorePermit>,
    permit_returned: Arc<Notify>,
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        // Drop the permit before notifying so `semaphore.available_permits()`
        // reflects the freed slot when the worker wakes up.
        drop(self.permit.take());
        self.permit_returned.notify_one();
    }
}

/// Run orphan recovery if at least `interval` has elapsed since `last_run`.
async fn maybe_recover_orphans(
    scheduler: &dyn TaskScheduler,
    orphan_timeout: Duration,
    interval: Duration,
    last_run: &mut tokio::time::Instant,
) {
    if last_run.elapsed() < interval {
        return;
    }
    *last_run = tokio::time::Instant::now();
    match scheduler.recover_orphans(orphan_timeout).await {
        Ok(n) if n > 0 => info!(recovered = n, "Orphan tasks recovered"),
        Ok(_) => {}
        Err(e) => error!(error = %e, "Orphan recovery failed"),
    }
}

/// Wait for a wakeup notification, fallback `poll_interval`, or shutdown/cancellation.
///
/// Returns `true` if the worker should continue polling, or `false` if shutdown
/// or cancellation was triggered.
async fn wait_idle(
    wakeup: Option<&Notify>,
    poll_interval: Duration,
    shutdown: &Notify,
    cancellation: &CancellationToken,
) -> bool {
    let shutdown_notified = shutdown.notified();
    let cancelled = cancellation.cancelled();
    tokio::pin!(shutdown_notified);
    tokio::pin!(cancelled);

    tokio::select! {
        biased;
        _ = &mut shutdown_notified => {
            info!("Scheduler worker shutdown signal received, exiting poll loop");
            false
        }
        _ = &mut cancelled => {
            info!("Scheduler worker cancellation token triggered, exiting poll loop");
            false
        }
        _ = async {
            match wakeup {
                Some(w) => w.notified().await,
                None => std::future::pending().await,
            }
        } => true,
        _ = tokio::time::sleep(poll_interval) => true,
    }
}

/// Block until at least one concurrency permit is available, or return `None`
/// if shutdown/cancellation is triggered first.
async fn wait_for_capacity(
    semaphore: &Semaphore,
    permit_returned: &Notify,
    shutdown: &Notify,
    cancellation: &CancellationToken,
) -> Option<usize> {
    loop {
        let available = semaphore.available_permits();
        if available > 0 {
            return Some(available);
        }

        let shutdown_notified = shutdown.notified();
        let cancelled = cancellation.cancelled();
        let returned = permit_returned.notified();
        tokio::pin!(shutdown_notified);
        tokio::pin!(cancelled);
        tokio::pin!(returned);

        tokio::select! {
            biased;
            _ = &mut shutdown_notified => {
                info!("Scheduler worker shutdown signal received while waiting for capacity");
                return None;
            }
            _ = &mut cancelled => {
                info!("Scheduler worker cancellation token triggered while waiting for capacity");
                return None;
            }
            _ = &mut returned => {}
        }
    }
}

/// Background heartbeat loop that extends a task's lease until cancelled.
async fn heartbeat_loop(
    scheduler: Arc<dyn TaskScheduler>,
    task_id: String,
    lock_token: String,
    task_name: String,
    interval: Duration,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = tokio::time::sleep(interval) => {
                match scheduler.renew_lease(&task_id, &lock_token).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(%task_id, %task_name, "Heartbeat: lease no longer exists, stopping");
                        break;
                    }
                    Err(e) => {
                        error!(%task_id, error = %e, "Heartbeat: failed to renew lease");
                    }
                }
            }
        }
    }
}

/// Dispatch a single fetched task to its registered handler.
#[instrument(
    name = "scheduler.dispatch_task",
    skip(scheduler, registry, task),
    fields(
        task_id = %task.task_id,
        task_name = %task.task_name,
        attempt = task.attempt,
    )
)]
async fn dispatch_task(
    scheduler: Arc<dyn TaskScheduler>,
    registry: Arc<TaskRegistry>,
    task: FetchedTask,
    heartbeat_interval: Option<Duration>,
    orphan_timeout: Duration,
) {
    let handler = match registry.get(&task.task_name) {
        Some(h) => h,
        None => {
            warn!(
                "No handler registered for task '{}'; registered handlers: [{}]",
                task.task_name,
                registry.handler_names().join(", ")
            );
            let _ = scheduler
                .mark_failed(
                    &task.task_id,
                    "no handler registered",
                    None,
                    &task.lock_token,
                )
                .await;
            return;
        }
    };

    let instance = TaskInstance {
        task_id: task.task_id.clone(),
        task_name: task.task_name.clone(),
        data: task.data,
        metadata: task.metadata,
        lock_token: task.lock_token.clone(),
        attempt: task.attempt as u32,
    };

    // Start heartbeat
    let hb_cancel = CancellationToken::new();
    let effective_interval = heartbeat_interval
        .filter(|d| !d.is_zero())
        .unwrap_or_else(|| orphan_timeout / 3);
    let hb_handle = tokio::spawn({
        let scheduler = scheduler.clone();
        let task_id = task.task_id.clone();
        let lock_token = task.lock_token.clone();
        let task_name = task.task_name.clone();
        let cancel = hb_cancel.clone();
        async move {
            heartbeat_loop(
                scheduler,
                task_id,
                lock_token,
                task_name,
                effective_interval,
                cancel,
            )
            .await;
        }
    });

    // 1. Execute — no transaction opened by the worker
    let completion: Box<dyn CompletionHandler> = match handler.execute(&instance).await {
        Ok(c) => c,
        Err(e) => {
            hb_cancel.cancel();
            let _ = hb_handle.await;
            // Handler is responsible for cleaning up its own tx before returning Err.
            let _ = scheduler
                .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                .await;
            return;
        }
    };

    // 2. Stop heartbeat — handler has returned
    hb_cancel.cancel();
    let _ = hb_handle.await;

    // 3. Build ops and delegate entirely to the completion handler.
    //    The worker opens no transaction; ExecutionOps does so lazily when needed.
    let ops = ExecutionOps::new(scheduler.clone(), &task.task_id, &task.lock_token);
    if let Err(e) = completion
        .complete(ops, task.recurrence.as_ref(), task.execution_time)
        .await
    {
        error!(error = %e, "Completion handler failed");
        let _ = scheduler
            .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::*;
    use crate::completion::OnComplete;
    use crate::error::StorageError;
    use crate::task::{ScheduledTask, SchedulerTaskError};
    use crate::types::ScheduleResult;

    struct StubScheduler {
        queued: Mutex<VecDeque<FetchedTask>>,
        poll_limits: Mutex<Vec<usize>>,
        poll_fetched: Mutex<Vec<usize>>,
        poll_notified: Notify,
        wakeup: Option<Arc<Notify>>,
        recover_orphans_calls: AtomicUsize,
    }

    impl StubScheduler {
        fn with_tasks(tasks: Vec<FetchedTask>) -> Self {
            Self {
                queued: Mutex::new(tasks.into_iter().collect()),
                poll_limits: Mutex::new(Vec::new()),
                poll_fetched: Mutex::new(Vec::new()),
                poll_notified: Notify::new(),
                wakeup: None,
                recover_orphans_calls: AtomicUsize::new(0),
            }
        }

        fn with_wakeup(mut self, wakeup: Arc<Notify>) -> Self {
            self.wakeup = Some(wakeup);
            self
        }

        fn recorded_poll_limits(&self) -> Vec<usize> {
            self.poll_limits.lock().unwrap().clone()
        }

        fn recorded_poll_fetched(&self) -> Vec<usize> {
            self.poll_fetched.lock().unwrap().clone()
        }

        fn recover_orphans_count(&self) -> usize {
            self.recover_orphans_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TaskScheduler for StubScheduler {
        async fn schedule_now(
            &self,
            task_id: &str,
            _task_name: &str,
            _data: serde_json::Value,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time: Utc::now(),
            })
        }

        async fn schedule_at(
            &self,
            params: ScheduleAtParams,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: params.task_id,
                execution_time: params.execution_time,
            })
        }

        async fn poll_due(
            &self,
            _now: DateTime<Utc>,
            limit: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            self.poll_limits.lock().unwrap().push(limit);
            let mut queued = self.queued.lock().unwrap();
            let take = limit.min(queued.len());
            let batch: Vec<_> = queued.drain(..take).collect();
            drop(queued);
            self.poll_fetched.lock().unwrap().push(batch.len());
            self.poll_notified.notify_waiters();
            Ok(batch)
        }

        async fn update_task_state(
            &self,
            _task_id: &str,
            _state: serde_json::Value,
            _next_execution_time: DateTime<Utc>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_completed(
            &self,
            _task_id: &str,
            _result: Option<serde_json::Value>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_failed(
            &self,
            _task_id: &str,
            _error: &str,
            _next_execution_time: Option<DateTime<Utc>>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn cancel_task(&self, _task_id: &str) -> Result<bool, StorageError> {
            Ok(true)
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn recover_orphans(&self, _stale_timeout: Duration) -> Result<usize, StorageError> {
            self.recover_orphans_calls.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }

        async fn subscribe_wakeups(
            &self,
            _options: WakeupOptions,
            _cancel: CancellationToken,
        ) -> Option<Arc<Notify>> {
            self.wakeup.clone()
        }
    }

    struct GatedHandler {
        started_tx: mpsc::UnboundedSender<String>,
        gates: Mutex<HashMap<String, Arc<Notify>>>,
    }

    impl GatedHandler {
        fn new(started_tx: mpsc::UnboundedSender<String>, task_ids: &[&str]) -> Self {
            let gates = task_ids
                .iter()
                .map(|id| ((*id).to_string(), Arc::new(Notify::new())))
                .collect();
            Self {
                started_tx,
                gates: Mutex::new(gates),
            }
        }

        fn release(&self, task_id: &str) {
            let gate = self
                .gates
                .lock()
                .unwrap()
                .get(task_id)
                .cloned()
                .expect("unknown task_id gate");
            gate.notify_one();
        }
    }

    #[async_trait]
    impl ScheduledTask for GatedHandler {
        async fn execute(
            &self,
            instance: &TaskInstance,
        ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
            let gate = self
                .gates
                .lock()
                .unwrap()
                .get(&instance.task_id)
                .cloned()
                .expect("missing gate for task");
            let _ = self.started_tx.send(instance.task_id.clone());
            gate.notified().await;
            Ok(OnComplete::done())
        }
    }

    struct SharedHandler(Arc<GatedHandler>);

    #[async_trait]
    impl ScheduledTask for SharedHandler {
        async fn execute(
            &self,
            instance: &TaskInstance,
        ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
            self.0.execute(instance).await
        }
    }

    fn make_task(id: &str) -> FetchedTask {
        FetchedTask {
            task_id: id.to_string(),
            task_name: "gated".to_string(),
            data: json!({}),
            state: json!({}),
            attempt: 1,
            lock_token: format!("lock-{id}"),
            recurrence: None,
            metadata: json!({}),
            execution_time: Utc::now(),
        }
    }

    #[tokio::test]
    async fn saturated_worker_skips_poll_due_until_permit_returned() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(StubScheduler::with_tasks(vec![
            make_task("t1"),
            make_task("t2"),
            make_task("t3"),
        ]));
        let handler = Arc::new(GatedHandler::new(started_tx, &["t1", "t2", "t3"]));

        let mut registry = TaskRegistry::new();
        registry.register("gated", SharedHandler(handler.clone()));

        let config = WorkerConfig {
            poll_interval: Duration::from_millis(10),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 2,
            heartbeat_interval: Some(Duration::ZERO),
            ..WorkerConfig::default()
        };

        let worker = Arc::new(Worker::new(
            scheduler.clone(),
            Arc::new(registry),
            config,
            Vec::new(),
        ));
        let worker_handle = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        // Wait for both t1 and t2 to start executing, saturating max_concurrent_tasks = 2.
        let mut started = Vec::new();
        for _ in 0..2 {
            let id = tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .expect("timed out waiting for initial tasks")
                .expect("channel closed");
            started.push(id);
        }
        started.sort();
        assert_eq!(started, vec!["t1", "t2"]);

        // Let several poll_interval ticks elapse while saturated: poll_due must NOT be called again.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(scheduler.recorded_poll_limits(), vec![2]);

        // Release one task; permit_returned should wake the worker to poll t3 with limit == 1.
        handler.release("t1");
        let third = tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("timed out waiting for t3 after permit release")
            .expect("channel closed");
        assert_eq!(third, "t3");
        assert_eq!(scheduler.recorded_poll_limits(), vec![2, 1]);

        handler.release("t2");
        handler.release("t3");
        worker.stop();
        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("worker did not stop")
            .expect("worker task panicked");
    }

    #[tokio::test]
    async fn batch_limit_clamped_to_available_permits() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(StubScheduler::with_tasks(vec![
            make_task("t1"),
            make_task("t2"),
        ]));
        let handler = Arc::new(GatedHandler::new(started_tx, &["t1", "t2"]));

        let mut registry = TaskRegistry::new();
        registry.register("gated", SharedHandler(handler.clone()));

        let config = WorkerConfig {
            poll_interval: Duration::from_millis(10),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 3,
            heartbeat_interval: Some(Duration::ZERO),
            ..WorkerConfig::default()
        };

        let worker = Arc::new(Worker::new(
            scheduler.clone(),
            Arc::new(registry),
            config,
            Vec::new(),
        ));
        let worker_handle = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        // First poll has 3 available permits, so limit = min(10, 3) = 3 and fetches t1, t2.
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .expect("timed out waiting for tasks")
                .expect("channel closed");
        }

        // With 2 tasks still running, only 1 permit remains; wait for the second poll_due call.
        tokio::time::timeout(Duration::from_secs(2), async {
            while scheduler.recorded_poll_limits().len() < 2 {
                scheduler.poll_notified.notified().await;
            }
        })
        .await
        .expect("timed out waiting for second poll_due");

        assert_eq!(scheduler.recorded_poll_limits()[..2], [3, 1]);

        handler.release("t1");
        handler.release("t2");
        worker.stop();
        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("worker did not stop")
            .expect("worker task panicked");
    }

    struct ImmediateHandler {
        executed: Arc<AtomicUsize>,
        target: usize,
        done_notify: Arc<Notify>,
    }

    #[async_trait]
    impl ScheduledTask for ImmediateHandler {
        async fn execute(
            &self,
            _instance: &TaskInstance,
        ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
            if self.executed.fetch_add(1, Ordering::SeqCst) + 1 >= self.target {
                self.done_notify.notify_waiters();
            }
            Ok(OnComplete::done())
        }
    }

    #[tokio::test]
    async fn drain_mode_drains_all_batches_without_sleeping_poll_interval() {
        let tasks: Vec<_> = (0..25).map(|i| make_task(&format!("t{i}"))).collect();
        let scheduler = Arc::new(StubScheduler::with_tasks(tasks));
        let executed = Arc::new(AtomicUsize::new(0));
        let done_notify = Arc::new(Notify::new());

        let mut registry = TaskRegistry::new();
        registry.register(
            "gated",
            ImmediateHandler {
                executed: executed.clone(),
                target: 25,
                done_notify: done_notify.clone(),
            },
        );

        let config = WorkerConfig {
            poll_interval: Duration::from_secs(30),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 32,
            heartbeat_interval: Some(Duration::ZERO),
            ..WorkerConfig::default()
        };

        let worker = Arc::new(Worker::new(
            scheduler.clone(),
            Arc::new(registry),
            config,
            Vec::new(),
        ));
        let worker_handle = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        // All 25 tasks must drain in well under 30s (proving no poll_interval sleep occurred).
        tokio::time::timeout(Duration::from_secs(2), async {
            while executed.load(Ordering::SeqCst) < 25 {
                done_notify.notified().await;
            }
        })
        .await
        .expect("worker did not drain 25 tasks immediately");

        assert_eq!(scheduler.recorded_poll_fetched(), vec![10, 10, 5]);

        worker.stop();
        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("worker did not stop")
            .expect("worker task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_wakeup_and_fallback_poll() {
        let wakeup = Arc::new(Notify::new());
        let scheduler = Arc::new(StubScheduler::with_tasks(Vec::new()).with_wakeup(wakeup.clone()));

        let config = WorkerConfig {
            poll_interval: Duration::from_secs(30),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 16,
            heartbeat_interval: Some(Duration::ZERO),
            ..WorkerConfig::default()
        };

        let worker = Arc::new(Worker::new(
            scheduler.clone(),
            Arc::new(TaskRegistry::new()),
            config,
            Vec::new(),
        ));
        let worker_handle = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        // Initial poll happens immediately.
        while scheduler.recorded_poll_limits().is_empty() {
            scheduler.poll_notified.notified().await;
        }
        assert_eq!(scheduler.recorded_poll_limits().len(), 1);

        // Yield so the worker parks inside wait_idle.
        tokio::task::yield_now().await;

        // Notify wakeup: worker must poll immediately without waiting 30s.
        let before_wakeup = tokio::time::Instant::now();
        wakeup.notify_one();
        while scheduler.recorded_poll_limits().len() < 2 {
            scheduler.poll_notified.notified().await;
        }
        assert!(before_wakeup.elapsed() < Duration::from_secs(1));

        // Yield so the worker parks inside wait_idle again.
        tokio::task::yield_now().await;

        // No notification arrives: advancing paused time by 30s triggers fallback poll.
        tokio::time::advance(Duration::from_secs(30)).await;
        while scheduler.recorded_poll_limits().len() < 3 {
            scheduler.poll_notified.notified().await;
        }
        assert_eq!(scheduler.recorded_poll_limits().len(), 3);

        worker.stop();
        worker_handle.await.expect("worker task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn instant_based_recover_orphans_does_not_burst_during_drain() {
        // 495 tasks with max_tasks_per_poll = 10 -> 50 drain cycles (49 * 10 + 5).
        let tasks: Vec<_> = (0..495).map(|i| make_task(&format!("t{i}"))).collect();
        let scheduler = Arc::new(StubScheduler::with_tasks(tasks));
        let executed = Arc::new(AtomicUsize::new(0));
        let done_notify = Arc::new(Notify::new());

        let mut registry = TaskRegistry::new();
        registry.register(
            "gated",
            ImmediateHandler {
                executed: executed.clone(),
                target: 495,
                done_notify: done_notify.clone(),
            },
        );

        let config = WorkerConfig {
            poll_interval: Duration::from_secs(30),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 32,
            orphan_timeout: Duration::from_secs(300), // interval = 150s
            heartbeat_interval: Some(Duration::ZERO),
            ..WorkerConfig::default()
        };

        let worker = Arc::new(Worker::new(
            scheduler.clone(),
            Arc::new(registry),
            config,
            Vec::new(),
        ));
        let worker_handle = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        while executed.load(Ordering::SeqCst) < 495 {
            done_notify.notified().await;
        }

        // All 50 cycles ran within < 2s; recover_orphans must not burst (<= 1, actually 0).
        assert_eq!(scheduler.recorded_poll_fetched().len(), 50);
        assert!(scheduler.recover_orphans_count() <= 1);

        // Advancing past the 150s interval triggers recover_orphans on the next fallback poll.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(150)).await;
        while scheduler.recover_orphans_count() < 1 {
            scheduler.poll_notified.notified().await;
        }
        assert_eq!(scheduler.recover_orphans_count(), 1);

        worker.stop();
        worker_handle.await.expect("worker task panicked");
    }
}
