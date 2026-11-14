//! Implementation of the [`TaskScheduler`] trait for [`PostgresTaskScheduler`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::PostgresTaskScheduler;
use super::sql_helpers::{
    mark_completed_sql, mark_failed_sql, notify_wakeup_sql, schedule_at_sql,
    should_notify_on_schedule, update_task_state_sql,
};
use crate::{
    CompleteAndScheduleParams, FetchedTask, ScheduleAtParams, ScheduleResult, StorageError,
    TaskScheduler, WakeupOptions,
};

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

#[derive(serde::Deserialize)]
struct WakeupPayload<'a> {
    queue: Option<&'a str>,
}

/// Returns `true` if a `pg_notify` payload should wake a worker subscribed to `subscribed_queues`.
///
/// When `subscribed_queues` is empty, every notification wakes the worker.
/// When `subscribed_queues` is non-empty, only JSON payloads of the form
/// `{"queue":"<name>"}` matching one of `subscribed_queues` wake the worker;
/// malformed or empty payloads are ignored without panicking.
pub(crate) fn should_wake_for_payload(payload: &str, subscribed_queues: &[String]) -> bool {
    if subscribed_queues.is_empty() {
        return true;
    }

    match serde_json::from_str::<WakeupPayload<'_>>(payload) {
        Ok(WakeupPayload { queue: Some(q) }) => subscribed_queues.iter().any(|sub| sub == q),
        Ok(WakeupPayload { queue: None }) => false,
        Err(err) => {
            tracing::debug!(error = %err, "ignoring malformed pg_notify wakeup payload");
            false
        }
    }
}

fn spawn_local_wakeup_bridge(
    local_wakeup: Arc<Notify>,
    wakeup: Arc<Notify>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = local_wakeup.notified() => wakeup.notify_one(),
            }
        }
    });
}

async fn connect_and_listen(
    pool: &sqlx::PgPool,
    channel: &str,
    cancel: &CancellationToken,
) -> Result<Option<PgListener>, sqlx::Error> {
    let mut listener = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(None),
        res = PgListener::connect_with(pool) => res?,
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(None),
        res = listener.listen(channel) => res?,
    }
    Ok(Some(listener))
}

fn spawn_pg_listener_loop(
    pool: sqlx::PgPool,
    channel: String,
    subscribed_queues: Vec<String>,
    wakeup: Arc<Notify>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut backoff = INITIAL_RECONNECT_BACKOFF;
        let mut is_reconnect = false;

        loop {
            let mut listener = match connect_and_listen(&pool, &channel, &cancel).await {
                Ok(Some(listener)) => listener,
                Ok(None) => break,
                Err(err) => {
                    tracing::warn!(error = %err, channel = %channel, "PgListener connect/listen failed; retrying");
                    is_reconnect = true;
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                    continue;
                }
            };

            backoff = INITIAL_RECONNECT_BACKOFF;
            if is_reconnect {
                // D5: immediate wakeup on reconnect so poll_due reconciles missed notifications.
                wakeup.notify_one();
            }
            is_reconnect = true;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    res = listener.try_recv() => {
                        match res {
                            Ok(Some(notification)) => {
                                if should_wake_for_payload(notification.payload(), &subscribed_queues) {
                                    wakeup.notify_one();
                                }
                            }
                            Ok(None) => {
                                tracing::warn!(channel = %channel, "PgListener connection dropped; reconnecting");
                                break;
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, channel = %channel, "PgListener receive error; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
        }
    });
}

#[async_trait]
impl TaskScheduler for PostgresTaskScheduler {
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        self.schedule_at(ScheduleAtParams {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: Utc::now(),
            data,
            recurrence: None,
            metadata: serde_json::Value::Null,
        })
        .await
    }

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let result = schedule_at_sql(&mut tx, &params, &self.table_names).await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        if params.execution_time <= Utc::now() {
            self.notify_local_wakeup();
        }
        Ok(result)
    }

    /// Schedule a task within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    async fn schedule_at_in_tx(
        &self,
        conn: &mut PgConnection,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        schedule_at_sql(conn, &params, &self.table_names).await
    }

    async fn poll_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // SELECT with SKIP LOCKED to avoid contention between concurrent workers.
        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            i32,
            Option<serde_json::Value>,
            serde_json::Value,
            DateTime<Utc>,
        )> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            r#"
                SELECT task_id, task_name, data, state, attempt, recurrence, metadata, execution_time
                FROM {tasks}
                WHERE status = 'scheduled'
                  AND execution_time <= $1
                ORDER BY execution_time ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        if rows.is_empty() {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Ok(vec![]);
        }

        let mut fetched = Vec::with_capacity(rows.len());

        for (task_id, task_name, data, state, attempt, recurrence_json, metadata, execution_time) in
            rows
        {
            // Each task gets a unique lock token stored as `worker_id`.
            let lock_token = Uuid::new_v4().to_string();

            sqlx::query(sqlx::AssertSqlSafe(format!(
                r#"
                UPDATE {tasks}
                SET status     = 'picked_up',
                    locked_at  = NOW(),
                    worker_id  = $1,
                    attempt    = attempt + 1,
                    updated_at = NOW()
                WHERE task_id = $2
                "#,
                tasks = self.table_names.tasks(),
            )))
            .bind(&lock_token)
            .bind(&task_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

            let recurrence = recurrence_json.and_then(|v| serde_json::from_value(v).ok());

            fetched.push(FetchedTask {
                task_id,
                task_name,
                data,
                state,
                // Return the post-increment attempt count.
                attempt: attempt as usize + 1,
                lock_token,
                recurrence,
                metadata,
                execution_time,
            });
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(fetched)
    }

    async fn update_task_state(
        &self,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        update_task_state_sql(
            &mut conn,
            task_id,
            state,
            next_execution_time,
            lock_token,
            &self.table_names,
            None,
        )
        .await
    }

    async fn update_task_state_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
        metadata_patch: Option<&serde_json::Value>,
    ) -> Result<(), StorageError> {
        update_task_state_sql(
            conn,
            task_id,
            state,
            next_execution_time,
            lock_token,
            &self.table_names,
            metadata_patch,
        )
        .await
    }

    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        // For recurring tasks with no explicit result, reschedule instead of completing —
        // mirrors what OnComplete::complete() does via ExecutionOps.
        if result.is_none() {
            let row: Option<(Option<serde_json::Value>, DateTime<Utc>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT recurrence, execution_time FROM {tasks} WHERE task_id = $1 AND worker_id = $2",
                tasks = self.table_names.tasks(),
            )))
            .bind(task_id)
            .bind(lock_token)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

            if let Some((Some(rec_json), exec_time)) = row
                && let Ok(recurrence) = serde_json::from_value::<crate::Recurrence>(rec_json)
                && let Some(next_time) = recurrence.next_after(exec_time)
            {
                return self
                    .update_task_state(task_id, serde_json::Value::Null, next_time, lock_token)
                    .await;
            }
        }

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        mark_completed_sql(&mut conn, task_id, result, lock_token, &self.table_names).await
    }

    async fn mark_completed_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        mark_completed_sql(conn, task_id, result, lock_token, &self.table_names).await
    }

    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        mark_failed_sql(
            &mut conn,
            task_id,
            error,
            next_execution_time,
            lock_token,
            &self.table_names,
        )
        .await
    }

    async fn mark_failed_in_tx(
        &self,
        conn: &mut PgConnection,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        mark_failed_sql(
            conn,
            task_id,
            error,
            next_execution_time,
            lock_token,
            &self.table_names,
        )
        .await
    }

    async fn cancel_task(&self, task_id: &str) -> Result<bool, StorageError> {
        let rows_affected = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            UPDATE {tasks}
            SET status = 'cancelled', updated_at = NOW()
            WHERE task_id = $1 AND status = 'scheduled'
            "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {tasks} WHERE task_id = $1",
            tasks = self.table_names.tasks(),
        )))
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn recover_orphans(
        &self,
        stale_timeout: std::time::Duration,
    ) -> Result<usize, StorageError> {
        let threshold = Utc::now()
            - chrono::Duration::from_std(stale_timeout).unwrap_or(chrono::Duration::seconds(300));

        let (recovered_count, has_due): (i64, bool) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            r#"
                WITH recovered AS (
                    UPDATE {tasks}
                    SET status     = 'scheduled',
                        locked_at  = NULL,
                        worker_id  = NULL,
                        updated_at = NOW()
                    WHERE status    = 'picked_up'
                      AND locked_at < $1
                    RETURNING execution_time
                )
                SELECT
                    COUNT(*)::BIGINT,
                    COALESCE(BOOL_OR(execution_time <= NOW()), FALSE)
                FROM recovered
                "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(threshold)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        if has_due {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            notify_wakeup_sql(&mut conn, &self.table_names).await?;
        }

        Ok(recovered_count as usize)
    }

    async fn renew_lease(&self, task_id: &str, lock_token: &str) -> Result<bool, StorageError> {
        let rows_affected = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            UPDATE {tasks}
            SET locked_at  = NOW(),
                updated_at = NOW()
            WHERE task_id   = $1
              AND worker_id = $2
              AND status    = 'picked_up'
            "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(task_id)
        .bind(lock_token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn complete_and_schedule(
        &self,
        params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            UPDATE {tasks}
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW(),
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id   = $2
              AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(&params.result)
        .bind(&params.completed_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(params.completed_task_id));
        }

        let inserted_rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            INSERT INTO {tasks}
                (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
            tasks = self.table_names.tasks(),
        )))
        .bind(&params.new_task_id)
        .bind(&params.new_task_name)
        .bind(params.new_execution_time)
        .bind(&params.new_data)
        .bind(&params.new_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if should_notify_on_schedule(inserted_rows, params.new_execution_time, Utc::now()) {
            notify_wakeup_sql(&mut tx, &self.table_names).await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        if params.new_execution_time <= Utc::now() {
            self.notify_local_wakeup();
        }
        Ok(())
    }

    async fn begin(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StorageError> {
        self.pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }

    async fn subscribe_wakeups(
        &self,
        options: WakeupOptions,
        cancel: CancellationToken,
    ) -> Option<Arc<Notify>> {
        if !options.enable_listen_notify {
            // In-process local_wakeup remains active even when LISTEN/NOTIFY is disabled.
            return Some(Arc::clone(&self.local_wakeup));
        }

        let wakeup = Arc::new(Notify::new());
        spawn_local_wakeup_bridge(
            Arc::clone(&self.local_wakeup),
            Arc::clone(&wakeup),
            cancel.clone(),
        );

        let listener_pool = match self.listener_options.clone() {
            Some(opts) => PgPoolOptions::new()
                .max_connections(1)
                .connect_lazy_with(opts),
            None => self.pool.clone(),
        };

        spawn_pg_listener_loop(
            listener_pool,
            self.table_names.wakeup_channel().to_string(),
            options.subscribed_queues,
            Arc::clone(&wakeup),
            cancel,
        );

        Some(wakeup)
    }

    fn notify_local_wakeup(&self) {
        self.local_wakeup.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::PgPool;

    use super::*;
    use crate::postgres::TableNames;

    #[tokio::test]
    async fn notify_local_wakeup_signals_notifier() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart_test")
            .expect("lazy pool creation should succeed");
        let scheduler = PostgresTaskScheduler::new(pool);

        // No permit before notify_local_wakeup is called.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                scheduler.local_wakeup().notified()
            )
            .await
            .is_err()
        );

        // Calling notify_local_wakeup stores a permit for the next waiter.
        scheduler.notify_local_wakeup();
        tokio::time::timeout(
            Duration::from_millis(50),
            scheduler.local_wakeup().notified(),
        )
        .await
        .expect("local_wakeup should resolve immediately after notify_local_wakeup");
    }

    #[tokio::test]
    async fn with_table_names_initializes_local_wakeup() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart_test")
            .expect("lazy pool creation should succeed");
        let scheduler = PostgresTaskScheduler::with_table_names(pool, TableNames::default());

        let wakeup = scheduler.local_wakeup().clone();
        let notified = wakeup.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        scheduler.notify_local_wakeup();
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .expect("pinned notified future should wake on notify_local_wakeup");
    }

    #[test]
    fn should_wake_for_payload_empty_subscribed_queues_wakes_on_everything() {
        let subscribed: Vec<String> = vec![];
        assert!(should_wake_for_payload(
            r#"{"queue":"default"}"#,
            &subscribed
        ));
        assert!(should_wake_for_payload(
            r#"{"queue":"reports"}"#,
            &subscribed
        ));
        assert!(should_wake_for_payload("", &subscribed));
        assert!(should_wake_for_payload("not-json", &subscribed));
    }

    #[test]
    fn should_wake_for_payload_filters_by_subscribed_queues() {
        let subscribed = vec!["reports".to_string(), "emails".to_string()];
        assert!(!should_wake_for_payload(
            r#"{"queue":"default"}"#,
            &subscribed
        ));
        assert!(should_wake_for_payload(
            r#"{"queue":"reports"}"#,
            &subscribed
        ));
        assert!(should_wake_for_payload(
            r#"{"queue":"emails","extra":1}"#,
            &subscribed
        ));
        assert!(!should_wake_for_payload("", &subscribed));
        assert!(!should_wake_for_payload("not-json", &subscribed));
        assert!(!should_wake_for_payload(
            r#"{"other":"reports"}"#,
            &subscribed
        ));
        assert!(!should_wake_for_payload(r#"{"queue":null}"#, &subscribed));
        assert!(!should_wake_for_payload(r#"{"queue":123}"#, &subscribed));
    }

    #[tokio::test]
    async fn with_listener_options_and_url_configure_scheduler() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart_test")
            .expect("lazy pool creation should succeed");

        let default_scheduler = PostgresTaskScheduler::new(pool.clone());
        assert!(default_scheduler.listener_options().is_none());

        let opts = sqlx::postgres::PgConnectOptions::new()
            .host("direct.pg.internal")
            .port(5433)
            .database("app_db");
        let with_opts = PostgresTaskScheduler::new(pool.clone()).with_listener_options(opts);
        let configured = with_opts.listener_options().expect("options should be set");
        assert_eq!(configured.get_host(), "direct.pg.internal");
        assert_eq!(configured.get_port(), 5433);
        assert_eq!(configured.get_database(), Some("app_db"));

        let with_url = PostgresTaskScheduler::new(pool.clone())
            .with_listener_url("postgres://user:pass@db.internal:5434/zart_direct")
            .expect("valid URL should parse");
        let url_opts = with_url
            .listener_options()
            .expect("URL options should be set");
        assert_eq!(url_opts.get_host(), "db.internal");
        assert_eq!(url_opts.get_port(), 5434);
        assert_eq!(url_opts.get_database(), Some("zart_direct"));

        let invalid = PostgresTaskScheduler::new(pool).with_listener_url("not a valid url");
        assert!(matches!(invalid, Err(StorageError::Database(_))));
    }

    #[tokio::test]
    async fn subscribe_wakeups_bridges_local_wakeup_when_listen_notify_disabled() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart_test")
            .expect("lazy pool creation should succeed");
        let scheduler = PostgresTaskScheduler::new(pool);
        let cancel = CancellationToken::new();

        let wakeup = scheduler
            .subscribe_wakeups(
                WakeupOptions {
                    enable_listen_notify: false,
                    subscribed_queues: vec![],
                },
                cancel.clone(),
            )
            .await
            .expect("subscribe_wakeups should return Some even when LISTEN/NOTIFY is disabled");

        let notified = wakeup.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        scheduler.notify_local_wakeup();
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .expect("local_wakeup should wake subscriber when LISTEN/NOTIFY is disabled");

        cancel.cancel();
    }

    #[tokio::test]
    async fn subscribe_wakeups_bridges_local_wakeup_when_listen_notify_enabled() {
        // Use an unused localhost port so PgListener retries in the background
        // without blocking local_wakeup bridging.
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@127.0.0.1:1/zart_test")
            .expect("lazy pool creation should succeed");
        let scheduler = PostgresTaskScheduler::new(pool);
        let cancel = CancellationToken::new();

        let wakeup = scheduler
            .subscribe_wakeups(
                WakeupOptions {
                    enable_listen_notify: true,
                    subscribed_queues: vec!["reports".to_string()],
                },
                cancel.clone(),
            )
            .await
            .expect("subscribe_wakeups should return Some when LISTEN/NOTIFY is enabled");

        let notified = wakeup.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        scheduler.notify_local_wakeup();
        tokio::time::timeout(Duration::from_millis(100), notified)
            .await
            .expect(
                "local_wakeup bridge should wake subscriber even while PgListener is reconnecting",
            );

        cancel.cancel();
    }
}
