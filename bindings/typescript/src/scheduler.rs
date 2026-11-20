use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::backend::PgBackend;
use crate::errors::{backend_err, scheduler_err, schema_err};
use crate::trx::ClientTrx;

fn secs(v: Option<f64>, default: f64, name: &str) -> Result<Duration> {
    Duration::try_from_secs_f64(v.unwrap_or(default))
        .map_err(|e| schema_err(format!("invalid {name}: {e}")))
}

fn parse_rfc3339(s: &str, field: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&chrono::Utc))
        .map_err(|e| schema_err(format!("invalid RFC3339 timestamp for {field}: {e}")))
}

fn non_neg_usize(v: Option<i64>, default: usize, field: &str) -> Result<usize> {
    match v {
        None => Ok(default),
        Some(n) if n < 0 => Err(schema_err(format!("{field} must be >= 0"))),
        Some(n) => usize::try_from(n).map_err(|_| schema_err(format!("{field} out of range"))),
    }
}

fn trigger_to_str(t: zart_core::types::ExecutionTrigger) -> String {
    match t {
        zart_core::types::ExecutionTrigger::Initial => "initial".into(),
        zart_core::types::ExecutionTrigger::Restart => "restart".into(),
        zart_core::types::ExecutionTrigger::SelectiveRerun => "selective_rerun".into(),
    }
}

fn step_kind_to_str(k: zart_core::types::StepKind) -> String {
    match k {
        zart_core::types::StepKind::Step => "step".into(),
        zart_core::types::StepKind::Sleep => "sleep".into(),
        zart_core::types::StepKind::WaitAll => "wait_all".into(),
        zart_core::types::StepKind::WaitForEvent => "wait_for_event".into(),
        zart_core::types::StepKind::WaitGroup => "wait_group".into(),
        zart_core::types::StepKind::Capture => "capture".into(),
    }
}

fn step_status_to_str(s: zart_core::types::StepStatus) -> String {
    match s {
        zart_core::types::StepStatus::Scheduled => "scheduled".into(),
        zart_core::types::StepStatus::Running => "running".into(),
        zart_core::types::StepStatus::Completed => "completed".into(),
        zart_core::types::StepStatus::Dead => "dead".into(),
    }
}

fn step_attempt_status_to_str(s: zart_core::types::StepAttemptStatus) -> String {
    match s {
        zart_core::types::StepAttemptStatus::Completed => "completed".into(),
        zart_core::types::StepAttemptStatus::Failed => "failed".into(),
    }
}

#[napi(object)]
pub struct ScheduleResult {
    pub task_id: String,
    /// RFC3339 timestamp.
    pub execution_time: String,
}

#[napi(object)]
pub struct ExecutionRecord {
    pub execution_id: String,
    pub task_name: String,
    pub payload: serde_json::Value,
    /// scheduled | running | completed | failed | cancelled
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub scheduled_at: String,
    pub completed_at: Option<String>,
    pub version: i32,
}

impl From<zart::ExecutionRecord> for ExecutionRecord {
    fn from(r: zart::ExecutionRecord) -> Self {
        Self {
            execution_id: r.execution_id,
            task_name: r.task_name,
            payload: r.payload,
            status: r.status.to_string(),
            result: r.result,
            scheduled_at: r.scheduled_at.to_rfc3339(),
            completed_at: r.completed_at.map(|d| d.to_rfc3339()),
            version: r.version,
        }
    }
}

/// Execution count statistics grouped by status.
#[napi(object)]
pub struct ExecutionStats {
    pub scheduled: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

impl From<zart_core::types::ExecutionStats> for ExecutionStats {
    fn from(s: zart_core::types::ExecutionStats) -> Self {
        Self {
            scheduled: s.scheduled,
            running: s.running,
            completed: s.completed,
            failed: s.failed,
            cancelled: s.cancelled,
        }
    }
}

/// Parameters for filtered, paginated execution listing.
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct ListExecutionsParams {
    /// Filter by status: `scheduled | running | completed | failed | cancelled`.
    pub status: Option<String>,
    /// Case-insensitive substring filter on `taskName`.
    pub task_name: Option<String>,
    /// Lower bound on `startedAt` (RFC3339 timestamp).
    pub from: Option<String>,
    /// Upper bound on `startedAt` (RFC3339 timestamp).
    pub to: Option<String>,
    /// Prefix filter on `executionId`.
    pub search: Option<String>,
    /// Sort field: `scheduled_at | status | task_name` (default: `scheduled_at`).
    pub sort_by: Option<String>,
    /// Sort direction: `asc | desc` (default: `desc`).
    pub sort_order: Option<String>,
    /// Maximum number of records to return (default: `20`).
    pub limit: Option<i64>,
    /// Number of records to skip (default: `0`).
    pub offset: Option<i64>,
}

impl TryFrom<ListExecutionsParams> for zart_core::types::ListExecutionsParams {
    type Error = Error;

    fn try_from(p: ListExecutionsParams) -> Result<Self> {
        let status = p
            .status
            .as_deref()
            .map(str::parse::<zart_core::types::ExecutionStatus>)
            .transpose()
            .map_err(schema_err)?;

        let from = p
            .from
            .as_deref()
            .map(|s| parse_rfc3339(s, "from"))
            .transpose()?;

        let to =
            p.to.as_deref()
                .map(|s| parse_rfc3339(s, "to"))
                .transpose()?;

        let sort_by = match p.sort_by.as_deref() {
            None => zart_core::types::ExecutionSortField::default(),
            Some("scheduled_at") => zart_core::types::ExecutionSortField::ScheduledAt,
            Some("status") => zart_core::types::ExecutionSortField::Status,
            Some("task_name") => zart_core::types::ExecutionSortField::TaskName,
            Some(other) => {
                return Err(schema_err(format!(
                    "invalid sortBy '{other}': expected 'scheduled_at', 'status', or 'task_name'"
                )));
            }
        };

        let sort_order = match p.sort_order.as_deref() {
            None => zart_core::types::SortOrder::default(),
            Some("asc") => zart_core::types::SortOrder::Asc,
            Some("desc") => zart_core::types::SortOrder::Desc,
            Some(other) => {
                return Err(schema_err(format!(
                    "invalid sortOrder '{other}': expected 'asc' or 'desc'"
                )));
            }
        };

        let limit = non_neg_usize(p.limit, 20, "limit")?;
        let offset = non_neg_usize(p.offset, 0, "offset")?;

        Ok(Self {
            status,
            task_name: p.task_name,
            from,
            to,
            search: p.search,
            sort_by,
            sort_order,
            limit,
            offset,
        })
    }
}

/// A single run of a durable execution.
#[napi(object)]
pub struct ExecutionRunRecord {
    pub run_id: String,
    pub execution_id: String,
    pub run_index: i32,
    pub payload: serde_json::Value,
    /// scheduled | running | completed | failed | cancelled
    pub status: String,
    pub result: Option<serde_json::Value>,
    /// RFC3339 timestamp.
    pub started_at: String,
    /// RFC3339 timestamp.
    pub completed_at: Option<String>,
    /// initial | restart | selective_rerun
    pub trigger: String,
}

impl From<zart_core::types::ExecutionRunRecord> for ExecutionRunRecord {
    fn from(r: zart_core::types::ExecutionRunRecord) -> Self {
        Self {
            run_id: r.run_id,
            execution_id: r.execution_id,
            run_index: r.run_index,
            payload: r.payload,
            status: r.status.to_string(),
            result: r.result,
            started_at: r.started_at.to_rfc3339(),
            completed_at: r.completed_at.map(|d| d.to_rfc3339()),
            trigger: trigger_to_str(r.trigger),
        }
    }
}

/// A step record from `zart_steps`.
#[napi(object)]
pub struct StepRow {
    pub step_id: String,
    pub run_id: String,
    pub step_name: String,
    /// step | sleep | wait_all | wait_for_event | wait_group | capture
    pub step_kind: String,
    pub task_id: Option<String>,
    /// scheduled | running | completed | dead
    pub status: String,
    pub retry_attempt: i32,
    pub retry_config: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub last_error: Option<String>,
    pub wg_total: Option<i32>,
    pub wg_remaining: Option<i32>,
    pub wg_threshold: Option<i32>,
    pub wg_first_failed: Option<bool>,
    /// RFC3339 timestamp.
    pub scheduled_at: String,
    /// RFC3339 timestamp.
    pub completed_at: Option<String>,
}

impl From<zart_core::types::StepRow> for StepRow {
    fn from(s: zart_core::types::StepRow) -> Self {
        Self {
            step_id: s.step_id,
            run_id: s.run_id,
            step_name: s.step_name,
            step_kind: step_kind_to_str(s.step_kind),
            task_id: s.task_id,
            status: step_status_to_str(s.status),
            retry_attempt: s.retry_attempt,
            retry_config: s.retry_config,
            result: s.result,
            last_error: s.last_error,
            wg_total: s.wg_total,
            wg_remaining: s.wg_remaining,
            wg_threshold: s.wg_threshold,
            wg_first_failed: s.wg_first_failed,
            scheduled_at: s.scheduled_at.to_rfc3339(),
            completed_at: s.completed_at.map(|d| d.to_rfc3339()),
        }
    }
}

/// A single attempt record from `zart_step_attempts`.
#[napi(object)]
pub struct StepAttemptRow {
    pub attempt_id: String,
    pub step_id: String,
    pub attempt_number: i32,
    /// RFC3339 timestamp.
    pub started_at: String,
    /// RFC3339 timestamp.
    pub completed_at: Option<String>,
    /// completed | failed
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

impl From<zart_core::types::StepAttemptRow> for StepAttemptRow {
    fn from(a: zart_core::types::StepAttemptRow) -> Self {
        Self {
            attempt_id: a.attempt_id,
            step_id: a.step_id,
            attempt_number: a.attempt_number,
            started_at: a.started_at.to_rfc3339(),
            completed_at: a.completed_at.map(|d| d.to_rfc3339()),
            status: step_attempt_status_to_str(a.status),
            result: a.result,
            error: a.error,
        }
    }
}

/// A step with its attempts and retryability flag.
#[napi(object)]
pub struct StepWithAttempts {
    pub step: StepRow,
    pub attempts: Vec<StepAttemptRow>,
    pub retryable: bool,
}

impl From<zart::admin::StepWithAttempts> for StepWithAttempts {
    fn from(s: zart::admin::StepWithAttempts) -> Self {
        Self {
            step: s.step.into(),
            attempts: s.attempts.into_iter().map(Into::into).collect(),
            retryable: s.retryable,
        }
    }
}

/// Full execution detail: record, runs, and steps with attempt history.
#[napi(object)]
pub struct ExecutionDetail {
    pub execution: ExecutionRecord,
    pub runs: Vec<ExecutionRunRecord>,
    pub steps: Vec<StepWithAttempts>,
}

impl From<zart::admin::ExecutionDetail> for ExecutionDetail {
    fn from(d: zart::admin::ExecutionDetail) -> Self {
        Self {
            execution: d.execution.into(),
            runs: d.runs.into_iter().map(Into::into).collect(),
            steps: d.steps.into_iter().map(Into::into).collect(),
        }
    }
}

/// Specification for which steps to rerun vs preserve in `rerunSteps`.
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct RerunSpec {
    /// Steps to force-rerun even if currently completed.
    pub force_rerun: Option<Vec<String>>,
    /// Completed steps to explicitly preserve (ignored for failed/dead steps).
    pub preserve: Option<Vec<String>>,
    /// Optional operator identifier for audit logging.
    pub triggered_by: Option<String>,
}

impl From<RerunSpec> for zart::admin::RerunSpec {
    fn from(s: RerunSpec) -> Self {
        Self {
            force_rerun: s.force_rerun.unwrap_or_default(),
            preserve: s.preserve.unwrap_or_default(),
            triggered_by: s.triggered_by,
        }
    }
}

/// A preserved step that may have a stale dependency on a rerun step.
#[napi(object)]
pub struct PotentiallyStaleDep {
    pub preserved_step: String,
    pub possibly_depends_on: Vec<String>,
}

impl From<zart::admin::PotentiallyStaleDep> for PotentiallyStaleDep {
    fn from(d: zart::admin::PotentiallyStaleDep) -> Self {
        Self {
            preserved_step: d.preserved_step,
            possibly_depends_on: d.possibly_depends_on,
        }
    }
}

/// Result of a selective rerun operation.
#[napi(object)]
pub struct RerunResult {
    pub new_run_number: u32,
    pub effective_rerun: Vec<String>,
    pub potentially_stale: Vec<PotentiallyStaleDep>,
}

impl From<zart::admin::RerunResult> for RerunResult {
    fn from(r: zart::admin::RerunResult) -> Self {
        Self {
            new_run_number: r.new_run_number,
            effective_rerun: r.effective_rerun,
            potentially_stale: r.potentially_stale.into_iter().map(Into::into).collect(),
        }
    }
}

/// Scope of a pause or resume operation.
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct PauseScope {
    /// Target a specific execution.
    pub execution_id: Option<String>,
    /// Target all executions of a given task name.
    pub task_name: Option<String>,
    /// Glob pattern for step names (e.g. `"send-*"`).
    pub step_pattern: Option<String>,
    /// Optional auto-expiry for the pause rule (RFC3339 timestamp).
    pub expires_at: Option<String>,
    /// Optional operator identifier for audit logging.
    pub triggered_by: Option<String>,
    /// Optional human-readable reason for this pause.
    pub reason: Option<String>,
}

impl TryFrom<PauseScope> for zart::admin::PauseScope {
    type Error = Error;

    fn try_from(s: PauseScope) -> Result<Self> {
        let expires_at = s
            .expires_at
            .as_deref()
            .map(|ts| parse_rfc3339(ts, "expiresAt"))
            .transpose()?;

        Ok(Self {
            execution_id: s.execution_id,
            task_name: s.task_name,
            step_pattern: s.step_pattern,
            expires_at,
            triggered_by: s.triggered_by,
            reason: s.reason,
        })
    }
}

impl From<zart::admin::PauseScope> for PauseScope {
    fn from(s: zart::admin::PauseScope) -> Self {
        Self {
            execution_id: s.execution_id,
            task_name: s.task_name,
            step_pattern: s.step_pattern,
            expires_at: s.expires_at.map(|d| d.to_rfc3339()),
            triggered_by: s.triggered_by,
            reason: s.reason,
        }
    }
}

/// A pause rule stored in the database.
#[napi(object)]
pub struct PauseRule {
    pub rule_id: String,
    pub scope: PauseScope,
    /// RFC3339 timestamp.
    pub created_at: String,
    /// RFC3339 timestamp (`undefined` if active).
    pub deleted_at: Option<String>,
    pub reason: Option<String>,
}

impl From<zart::admin::PauseRule> for PauseRule {
    fn from(r: zart::admin::PauseRule) -> Self {
        Self {
            rule_id: r.rule_id,
            scope: r.scope.into(),
            created_at: r.created_at.to_rfc3339(),
            deleted_at: r.deleted_at.map(|d| d.to_rfc3339()),
            reason: r.reason,
        }
    }
}

/// Result of a resume operation.
#[napi(object)]
pub struct ResumeResult {
    pub rules_deleted: u32,
}

impl From<zart::admin::ResumeResult> for ResumeResult {
    fn from(r: zart::admin::ResumeResult) -> Self {
        Self {
            rules_deleted: u32::try_from(r.rules_deleted).unwrap_or(u32::MAX),
        }
    }
}

/// Filter for listing pause rules.
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct PauseRuleFilter {
    pub execution_id: Option<String>,
    pub task_name: Option<String>,
    pub include_deleted: Option<bool>,
}

impl From<PauseRuleFilter> for zart_core::store::PauseRuleFilter {
    fn from(f: PauseRuleFilter) -> Self {
        Self {
            execution_id: f.execution_id,
            task_name: f.task_name,
            include_deleted: f.include_deleted.unwrap_or(false),
        }
    }
}

#[napi]
pub struct DurableScheduler {
    inner: Arc<zart::DurableScheduler>,
}

#[napi]
impl DurableScheduler {
    #[napi(factory)]
    pub fn from_backend(backend: &PgBackend) -> DurableScheduler {
        DurableScheduler {
            inner: Arc::new(zart::DurableScheduler::from_backend(&*backend.inner)),
        }
    }

    #[napi]
    pub async fn start(
        &self,
        execution_id: String,
        task_name: String,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult> {
        let r = self
            .inner
            .start(&execution_id, &task_name, payload)
            .await
            .map_err(scheduler_err)?;
        Ok(ScheduleResult {
            task_id: r.task_id,
            execution_time: r.execution_time.to_rfc3339(),
        })
    }

    /// Start a new durable execution atomically inside a caller-owned `ClientTrx`.
    #[napi(ts_return_type = "Promise<ScheduleResult>")]
    pub fn start_in_tx(
        &self,
        env: Env,
        tx: &ClientTrx,
        execution_id: String,
        task_name: String,
        payload: serde_json::Value,
    ) -> Result<Object> {
        let inner = self.inner.clone();
        let tx_arc = tx.tx.clone();
        env.spawn_future(async move {
            let mut guard = tx_arc.lock().await;
            let sqlx_tx = guard
                .as_mut()
                .ok_or_else(|| backend_err("startInTx", "transaction is already closed"))?;
            let r = inner
                .start_in_tx(sqlx_tx, &execution_id, &task_name, payload)
                .await
                .map_err(scheduler_err)?;
            Ok(ScheduleResult {
                task_id: r.task_id,
                execution_time: r.execution_time.to_rfc3339(),
            })
        })
    }

    #[napi]
    pub async fn status(&self, execution_id: String) -> Result<ExecutionRecord> {
        self.inner
            .status(&execution_id)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn wait(
        &self,
        execution_id: String,
        timeout_secs: Option<f64>,
        poll_interval_secs: Option<f64>,
    ) -> Result<ExecutionRecord> {
        let t = secs(timeout_secs, 30.0, "timeoutSecs")?;
        let p = poll_interval_secs
            .map(|s| secs(Some(s), 0.5, "pollIntervalSecs"))
            .transpose()?;
        self.inner
            .wait(&execution_id, t, p)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn wait_completion(
        &self,
        execution_id: String,
        timeout_secs: Option<f64>,
        poll_interval_secs: Option<f64>,
    ) -> Result<serde_json::Value> {
        let t = secs(timeout_secs, 30.0, "timeoutSecs")?;
        let p = poll_interval_secs
            .map(|s| secs(Some(s), 0.5, "pollIntervalSecs"))
            .transpose()?;
        self.inner
            .wait_completion(&execution_id, t, p)
            .await
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn cancel(&self, execution_id: String) -> Result<bool> {
        self.inner
            .cancel(&execution_id)
            .await
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn offer_event(
        &self,
        execution_id: String,
        event_name: String,
        payload: serde_json::Value,
    ) -> Result<()> {
        self.inner
            .offer_event(&execution_id, &event_name, payload)
            .await
            .map_err(scheduler_err)
    }

    /// Stop a recurring durable execution from scheduling future ticks (idempotent).
    #[napi]
    pub async fn stop_recurring(&self, task_id: String) -> Result<()> {
        self.inner
            .stop_recurring(&task_id)
            .await
            .map_err(scheduler_err)
    }

    /// List durable execution records with optional filters, sorting, and pagination.
    #[napi]
    pub async fn list_executions(
        &self,
        params: Option<ListExecutionsParams>,
    ) -> Result<Vec<ExecutionRecord>> {
        let parsed = params.unwrap_or_default().try_into()?;
        let rows = self
            .inner
            .list_executions(parsed)
            .await
            .map_err(scheduler_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Return aggregate execution counts grouped by status.
    #[napi]
    pub async fn stats(&self) -> Result<ExecutionStats> {
        self.inner
            .stats()
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    /// Return the current `runId` for a durable execution, or `null` if never started.
    #[napi]
    pub async fn get_current_run_id(&self, execution_id: String) -> Result<Option<String>> {
        self.inner
            .get_current_run_id(&execution_id)
            .await
            .map_err(scheduler_err)
    }

    /// List all runs for a durable execution.
    #[napi]
    pub async fn list_runs(&self, execution_id: String) -> Result<Vec<ExecutionRunRecord>> {
        let rows = self
            .inner
            .list_runs(&execution_id)
            .await
            .map_err(scheduler_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Return full execution detail: record, runs, and steps with attempt history.
    #[napi]
    pub async fn execution_detail(
        &self,
        execution_id: String,
        run_id: Option<String>,
    ) -> Result<ExecutionDetail> {
        self.inner
            .execution_detail(&execution_id, run_id.as_deref())
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    /// Retry a single dead step within the specified run, returning the new `taskId`.
    #[napi]
    pub async fn retry_step(
        &self,
        run_id: String,
        step_name: String,
        triggered_by: Option<String>,
    ) -> Result<String> {
        self.inner
            .retry_step(&run_id, &step_name, triggered_by.as_deref())
            .await
            .map_err(scheduler_err)
    }

    /// Retry a single dead step within the current run of an execution, returning the new `taskId`.
    #[napi]
    pub async fn retry_step_current_run(
        &self,
        execution_id: String,
        step_name: String,
        triggered_by: Option<String>,
    ) -> Result<String> {
        self.inner
            .retry_step_current_run(&execution_id, &step_name, triggered_by.as_deref())
            .await
            .map_err(scheduler_err)
    }

    /// Restart an entire execution from scratch, returning the new `runId`.
    #[napi]
    pub async fn restart(
        &self,
        execution_id: String,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<String>,
    ) -> Result<String> {
        self.inner
            .restart(&execution_id, new_payload, triggered_by.as_deref())
            .await
            .map_err(scheduler_err)
    }

    /// Selectively rerun a subset of steps while preserving others.
    #[napi]
    pub async fn rerun_steps(&self, execution_id: String, spec: RerunSpec) -> Result<RerunResult> {
        self.inner
            .rerun_steps(&execution_id, spec.into())
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    /// Create a pause rule.
    #[napi]
    pub async fn pause(&self, scope: PauseScope) -> Result<PauseRule> {
        let parsed: zart::admin::PauseScope = scope.try_into()?;
        self.inner
            .pause(parsed)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    /// Resume execution by soft-deleting matching pause rules.
    #[napi]
    pub async fn resume(
        &self,
        scope: PauseScope,
        deleted_by: Option<String>,
    ) -> Result<ResumeResult> {
        let mut parsed: zart::admin::PauseScope = scope.try_into()?;
        if deleted_by.is_some() {
            parsed.triggered_by = deleted_by;
        }
        self.inner
            .resume(parsed)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    /// Resume by soft-deleting a specific pause rule by ID.
    #[napi]
    pub async fn resume_rule_by_id(
        &self,
        rule_id: String,
        deleted_by: Option<String>,
    ) -> Result<ResumeResult> {
        let deleted = self
            .inner
            .resume_rule_by_id(&rule_id, deleted_by.as_deref())
            .await
            .map_err(scheduler_err)?;
        Ok(ResumeResult {
            rules_deleted: u32::from(deleted),
        })
    }

    /// List pause rules (active only by default, or including soft-deleted when `includeDeleted` is true).
    #[napi]
    pub async fn list_pause_rules(
        &self,
        filter: Option<PauseRuleFilter>,
    ) -> Result<Vec<PauseRule>> {
        let rows = self
            .inner
            .list_pause_rules(filter.map(Into::into))
            .await
            .map_err(scheduler_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_params_defaults_match_rust() {
        let rust_params: zart_core::types::ListExecutionsParams =
            ListExecutionsParams::default().try_into().unwrap();
        let expected = zart_core::types::ListExecutionsParams::default();
        assert_eq!(rust_params.status, expected.status);
        assert_eq!(rust_params.task_name, expected.task_name);
        assert_eq!(rust_params.from, expected.from);
        assert_eq!(rust_params.to, expected.to);
        assert_eq!(rust_params.search, expected.search);
        assert_eq!(rust_params.sort_by, expected.sort_by);
        assert_eq!(rust_params.sort_order, expected.sort_order);
        assert_eq!(rust_params.limit, expected.limit);
        assert_eq!(rust_params.offset, expected.offset);
    }

    #[test]
    fn list_params_parses_filters_and_rejects_invalid() {
        let p = ListExecutionsParams {
            status: Some("completed".into()),
            task_name: Some("order".into()),
            from: Some("2026-01-01T00:00:00Z".into()),
            to: Some("2026-01-02T00:00:00Z".into()),
            search: Some("exec-".into()),
            sort_by: Some("task_name".into()),
            sort_order: Some("asc".into()),
            limit: Some(50),
            offset: Some(10),
        };
        let parsed: zart_core::types::ListExecutionsParams = p.try_into().unwrap();
        assert_eq!(
            parsed.status,
            Some(zart_core::types::ExecutionStatus::Completed)
        );
        assert_eq!(parsed.task_name.as_deref(), Some("order"));
        assert!(parsed.from.is_some());
        assert!(parsed.to.is_some());
        assert_eq!(parsed.search.as_deref(), Some("exec-"));
        assert_eq!(
            parsed.sort_by,
            zart_core::types::ExecutionSortField::TaskName
        );
        assert_eq!(parsed.sort_order, zart_core::types::SortOrder::Asc);
        assert_eq!(parsed.limit, 50);
        assert_eq!(parsed.offset, 10);

        // Invalid status
        let bad_status = ListExecutionsParams {
            status: Some("bogus".into()),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(bad_status).is_err());

        // Invalid sort_by
        let bad_sort = ListExecutionsParams {
            sort_by: Some("unknown".into()),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(bad_sort).is_err());

        // Invalid sort_order
        let bad_order = ListExecutionsParams {
            sort_order: Some("sideways".into()),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(bad_order).is_err());

        // Negative limit / offset
        let neg_limit = ListExecutionsParams {
            limit: Some(-1),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(neg_limit).is_err());

        let neg_offset = ListExecutionsParams {
            offset: Some(-5),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(neg_offset).is_err());

        // Invalid timestamp
        let bad_from = ListExecutionsParams {
            from: Some("not-a-date".into()),
            ..Default::default()
        };
        assert!(zart_core::types::ListExecutionsParams::try_from(bad_from).is_err());
    }

    #[test]
    fn admin_types_convert_and_validate() {
        let spec: zart::admin::RerunSpec = RerunSpec {
            force_rerun: Some(vec!["step-b".into()]),
            preserve: None,
            triggered_by: Some("ops".into()),
        }
        .into();
        assert_eq!(spec.force_rerun, vec!["step-b"]);
        assert!(spec.preserve.is_empty());
        assert_eq!(spec.triggered_by.as_deref(), Some("ops"));

        let scope: zart::admin::PauseScope = PauseScope {
            execution_id: Some("e1".into()),
            task_name: None,
            step_pattern: Some("send-*".into()),
            expires_at: Some("2026-12-01T00:00:00Z".into()),
            triggered_by: Some("ops".into()),
            reason: Some("maintenance".into()),
        }
        .try_into()
        .unwrap();
        assert_eq!(scope.execution_id.as_deref(), Some("e1"));
        assert_eq!(scope.step_pattern.as_deref(), Some("send-*"));
        assert!(scope.expires_at.is_some());

        let bad_scope = PauseScope {
            expires_at: Some("invalid-ts".into()),
            ..Default::default()
        };
        assert!(zart::admin::PauseScope::try_from(bad_scope).is_err());

        let filter: zart_core::store::PauseRuleFilter = PauseRuleFilter::default().into();
        assert!(!filter.include_deleted);
    }
}
