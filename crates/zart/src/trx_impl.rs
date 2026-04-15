//! Transactional step completion via task-local storage.
//!
//! This module provides [`trx`], which allows a step to participate in an
//! atomic database transaction that encompasses both the step's own writes
//! and the framework's step-completion writes.
//!
//! # How it works
//!
//! 1. The step calls `zart::trx(&pool)` inside its `run()` method.
//! 2. The framework detects the registered transaction after `run()` returns.
//! 3. If present, the framework uses the same transaction for
//!    `complete_step_and_schedule_body`, then commits it.
//! 4. If `run()` returns an error, the framework rolls back the transaction.
//!
//! # Contract
//!
//! - `zart::trx` must be called at most once per step invocation.
//! - The caller must **not** commit or roll back the transaction — the framework
//!   owns the lifecycle after `trx()` returns.
//! - Keep the time between `trx()` and returning from `run()` short; holding a
//!   transaction across blocking I/O (e.g. HTTP calls) is the caller's
//!   responsibility and risks long-lived locks.
//!
//! # Example
//!
//! ```rust,ignore
//! #[zart_step("debit_account")]
//! async fn run(&self) -> Result<DebitResult, DebitError> {
//!     let tx = zart::trx(&self.pool).await?;
//!
//!     sqlx::query("UPDATE accounts SET balance = balance - $1 WHERE id = $2")
//!         .bind(self.amount)
//!         .bind(self.account_id)
//!         .execute(&mut **tx)
//!         .await?;
//!
//!     Ok(DebitResult { debited: self.amount })
//! }
//! ```

use std::sync::Arc;
use zart_scheduler::StorageError;

use crate::error::StepError;

// Type aliases for clarity.
type TrxMutex = tokio::sync::Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>;
type TrxArc = std::sync::Arc<TrxMutex>;

// A transaction registered for the current step invocation.
// Each step invocation wraps execution in `with_step_trx`, which scopes
// a fresh `Arc<tokio::sync::Mutex<Option<Transaction>>>` into this task-local.
tokio::task_local! {
    pub(crate) static STEP_TRX: TrxArc;
}

/// Register a transaction for atomic step completion.
///
/// This must be called from within a step's `run()` method (i.e. when the
/// execution phase is `Phase::Step`). It begins a transaction from the
/// provided pool and stores it in a task-local. After `run()` returns, the
/// framework will use this transaction for step completion and then commit it.
///
/// # Errors
///
/// - Returns [`StepError::Failed`] if called outside a step invocation
///   (e.g. in body mode or before the step lambda executes).
/// - Returns [`StepError::Failed`] if `trx` has already been called in the
///   current step invocation (double registration is prohibited).
///
/// # Important
///
/// Do **not** commit or roll back the returned transaction yourself. The
/// framework owns the lifecycle after this function returns. If `run()`
/// returns an error, the framework will roll back automatically.
pub async fn trx(pool: &sqlx::PgPool) -> Result<ZartTrx, StepError> {
    // Guard: must be in step phase.
    if !crate::local::is_step_phase() {
        return Err(StepError::Failed {
            step: "zart::trx".to_string(),
            reason: "trx() can only be called from within a step's run() method".to_string(),
        });
    }

    let arc = STEP_TRX.with(Arc::clone);

    // try_lock_owned() fails immediately if the lock is held — which only
    // happens if ZartTrx from a previous trx() call is still alive.
    // This is the double-call guard: no unsafe, no separate flag.
    let mut guard = arc
        .clone()
        .try_lock_owned()
        .map_err(|_| StepError::Failed {
            step: "zart::trx".to_string(),
            reason: "trx() was already called in this step invocation".to_string(),
        })?;

    // pool.begin() already returns Transaction<'static, Postgres>.
    let tx = pool.begin().await.map_err(|e| StepError::Failed {
        step: "zart::trx".to_string(),
        reason: format!("failed to begin transaction: {e}"),
    })?;

    *guard = Some(tx);

    Ok(ZartTrx { _arc: arc, guard })
}

/// A handle to a transaction registered via [`trx`].
///
/// Implements `Deref` and `DerefMut` targeting
/// `sqlx::Transaction<'static, Postgres>` so it can be passed directly to
/// `sqlx::query(...).execute(&mut **tx)`.
///
/// # Lifecycle
///
/// The transaction is owned by the framework after `trx()` returns.
/// - If the step's `run()` returns `Ok`, the framework commits the transaction
///   after recording step completion.
/// - If the step's `run()` returns `Err`, the framework rolls back the
///   transaction before proceeding with retry logic.
///
/// # Anti-patterns
///
/// - Do **not** call `tx.commit()` or `tx.rollback()` — the framework handles this.
/// - Do **not** call `zart::trx` more than once per step invocation.
/// - Avoid long-latency I/O (HTTP calls, external services) between `trx()` and
///   returning from `run()` — this holds a database transaction open.
#[derive(Debug)]
pub struct ZartTrx {
    /// Keeps the Arc alive so the framework can retrieve the transaction
    /// after ZartTrx is dropped at the end of run().
    _arc: TrxArc,
    /// Holds the exclusive lock for the duration of run().
    /// Dropped (lock released) when run() returns.
    guard: tokio::sync::OwnedMutexGuard<Option<sqlx::Transaction<'static, sqlx::Postgres>>>,
}

impl std::ops::Deref for ZartTrx {
    type Target = sqlx::Transaction<'static, sqlx::Postgres>;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("ZartTrx deref: transaction not present")
    }
}

impl std::ops::DerefMut for ZartTrx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("ZartTrx deref_mut: transaction not present")
    }
}

/// Execute a future with the `STEP_TRX` task-local initialized.
///
/// This is called by the framework's `execute_step` path to set up the
/// task-local before the step's `run()` executes.
pub(crate) async fn with_step_trx<F, R>(f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let arc: TrxArc = Arc::new(tokio::sync::Mutex::new(None));
    STEP_TRX.scope(arc, f).await
}

/// Take the registered transaction (if any) from the task-local.
///
/// Returns `Some(tx)` if `trx()` was called during the step invocation,
/// or `None` if the step did not register a transaction or the task-local
/// wasn't initialized (backward compat with non-postgres builds).
///
/// This function is `async` because `OwnedMutexGuard` is acquired via
/// `lock_owned().await`. In practice the await **never blocks**: by the time
/// the framework calls `take_step_trx()`, the step's `run()` has already
/// returned and `ZartTrx` (which held the lock) has been dropped, so the
/// mutex is always uncontended.
pub(crate) async fn take_step_trx() -> Option<sqlx::Transaction<'static, sqlx::Postgres>> {
    let arc = STEP_TRX.try_with(Arc::clone).ok()?;
    let mut guard = arc.lock_owned().await;
    guard.take()
}

/// Roll back and discard the registered transaction (if any).
pub(crate) async fn rollback_trx() -> Result<(), StorageError> {
    if let Some(tx) = take_step_trx().await {
        tx.rollback().await.map_err(|e| {
            StorageError::Database(Box::new(sqlx::Error::Protocol(format!(
                "transaction rollback failed: {e}"
            ))))
        })?;
    }
    Ok(())
}

/// Commit the registered transaction (if any).
pub(crate) async fn commit_trx(
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<(), StorageError> {
    tx.commit().await.map_err(|e| {
        StorageError::Database(Box::new(sqlx::Error::Protocol(format!(
            "transaction commit failed: {e}"
        ))))
    })
}
