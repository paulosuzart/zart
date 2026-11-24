use std::str::FromStr;
use std::sync::Arc;

use pyo3::exceptions::{PyLookupError, PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;

use crate::backend::PyPgBackend;
use crate::duration_from_secs;
use crate::serde_bridge::{json_to_py, py_to_json};
use crate::trx::PyClientTrx;

fn scheduler_err_to_py(err: zart::SchedulerError) -> PyErr {
    match err {
        zart::SchedulerError::WaitTimedOut(msg) => PyTimeoutError::new_err(msg),
        zart::SchedulerError::ExecutionNotFound(msg) => PyLookupError::new_err(msg),
        zart::SchedulerError::Serialization(e) => PyValueError::new_err(e.to_string()),
        zart::SchedulerError::Deserialization(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// Result returned when scheduling a durable execution.
#[pyclass(name = "ScheduleResult", get_all, frozen)]
pub struct PyScheduleResult {
    pub task_id: String,
    pub execution_time: String,
}

#[pymethods]
impl PyScheduleResult {
    fn __repr__(&self) -> String {
        format!(
            "ScheduleResult(task_id={:?}, execution_time={:?})",
            self.task_id, self.execution_time
        )
    }
}

/// Snapshot of a durable execution record.
#[pyclass(name = "ExecutionRecord", get_all, frozen)]
pub struct PyExecutionRecord {
    pub execution_id: String,
    pub task_name: String,
    pub payload: Py<PyAny>,
    pub status: String,
    pub result: Option<Py<PyAny>>,
    pub scheduled_at: String,
    pub completed_at: Option<String>,
    pub version: i32,
}

impl PyExecutionRecord {
    fn from_rust_py(py: Python<'_>, record: zart::ExecutionRecord) -> PyResult<Self> {
        let payload = json_to_py(py, &record.payload)?.unbind();
        let result = match record.result {
            Some(ref val) => Some(json_to_py(py, val)?.unbind()),
            None => None,
        };
        Ok(Self {
            execution_id: record.execution_id,
            task_name: record.task_name,
            payload,
            status: record.status.to_string(),
            result,
            scheduled_at: record.scheduled_at.to_rfc3339(),
            completed_at: record.completed_at.map(|dt| dt.to_rfc3339()),
            version: record.version,
        })
    }

    fn from_rust(record: zart::ExecutionRecord) -> PyResult<Self> {
        Python::attach(|py| Self::from_rust_py(py, record))
    }
}

#[pymethods]
impl PyExecutionRecord {
    fn __repr__(&self) -> String {
        format!(
            "ExecutionRecord(execution_id={:?}, task_name={:?}, status={:?})",
            self.execution_id, self.task_name, self.status
        )
    }
}

fn parse_execution_status(s: &str) -> PyResult<zart::ExecutionStatus> {
    zart::ExecutionStatus::from_str(s).map_err(|_| {
        PyValueError::new_err(format!(
            "unknown execution status: {s:?}; expected 'scheduled', 'running', 'completed', 'failed', or 'cancelled'"
        ))
    })
}

fn parse_sort_by(s: &str) -> PyResult<zart::ExecutionSortField> {
    match s {
        "scheduled_at" => Ok(zart::ExecutionSortField::ScheduledAt),
        "status" => Ok(zart::ExecutionSortField::Status),
        "task_name" => Ok(zart::ExecutionSortField::TaskName),
        other => Err(PyValueError::new_err(format!(
            "unknown sort_by field: {other:?}; expected 'scheduled_at', 'status', or 'task_name'"
        ))),
    }
}

fn parse_sort_order(s: &str) -> PyResult<zart::SortOrder> {
    match s {
        "desc" => Ok(zart::SortOrder::Desc),
        "asc" => Ok(zart::SortOrder::Asc),
        other => Err(PyValueError::new_err(format!(
            "unknown sort_order: {other:?}; expected 'desc' or 'asc'"
        ))),
    }
}

fn parse_optional_datetime(
    obj: Option<&Bound<'_, PyAny>>,
    field: &str,
) -> PyResult<Option<String>> {
    let Some(val) = obj else {
        return Ok(None);
    };
    if val.is_none() {
        return Ok(None);
    }
    if let Ok(s) = val.extract::<String>() {
        chrono::DateTime::parse_from_rfc3339(&s)
            .map_err(|e| PyValueError::new_err(format!("invalid {field} datetime {s:?}: {e}")))?;
        return Ok(Some(s));
    }
    if let Ok(iso_method) = val.getattr("isoformat") {
        let iso_obj = if let Ok(tz) = val.getattr("tzinfo")
            && tz.is_none()
        {
            let dt_mod = val.py().import("datetime")?;
            let utc = dt_mod.getattr("timezone")?.getattr("utc")?;
            let kwargs = pyo3::types::PyDict::new(val.py());
            kwargs.set_item("tzinfo", utc)?;
            let aware = val.call_method("replace", (), Some(&kwargs))?;
            aware.call_method0("isoformat")?
        } else {
            iso_method.call0()?
        };
        let s: String = iso_obj.extract()?;
        chrono::DateTime::parse_from_rfc3339(&s)
            .map_err(|e| PyValueError::new_err(format!("invalid {field} datetime {s:?}: {e}")))?;
        return Ok(Some(s));
    }
    Err(PyValueError::new_err(format!(
        "{field} must be an ISO-8601 string or datetime.datetime"
    )))
}

fn execution_trigger_str(trigger: &zart_core::types::ExecutionTrigger) -> &'static str {
    match trigger {
        zart_core::types::ExecutionTrigger::Initial => "initial",
        zart_core::types::ExecutionTrigger::Restart => "restart",
        zart_core::types::ExecutionTrigger::SelectiveRerun => "selective_rerun",
    }
}

fn step_kind_str(kind: &zart_core::types::StepKind) -> &'static str {
    match kind {
        zart_core::types::StepKind::Step => "step",
        zart_core::types::StepKind::Sleep => "sleep",
        zart_core::types::StepKind::WaitAll => "wait_all",
        zart_core::types::StepKind::WaitForEvent => "wait_for_event",
        zart_core::types::StepKind::WaitGroup => "wait_group",
        zart_core::types::StepKind::Capture => "capture",
    }
}

fn step_status_str(status: &zart_core::types::StepStatus) -> &'static str {
    match status {
        zart_core::types::StepStatus::Scheduled => "scheduled",
        zart_core::types::StepStatus::Running => "running",
        zart_core::types::StepStatus::Completed => "completed",
        zart_core::types::StepStatus::Dead => "dead",
    }
}

fn step_attempt_status_str(status: &zart_core::types::StepAttemptStatus) -> &'static str {
    match status {
        zart_core::types::StepAttemptStatus::Completed => "completed",
        zart_core::types::StepAttemptStatus::Failed => "failed",
    }
}

/// Parameters for filtered, paginated execution listing.
#[pyclass(name = "ListExecutionsParams", get_all, frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyListExecutionsParams {
    pub status: Option<String>,
    pub task_name: Option<String>,
    pub from_: Option<String>,
    pub to: Option<String>,
    pub search: Option<String>,
    pub sort_by: String,
    pub sort_order: String,
    pub limit: usize,
    pub offset: usize,
}

impl PyListExecutionsParams {
    fn to_rust(&self) -> PyResult<zart::ListExecutionsParams> {
        let status = self
            .status
            .as_deref()
            .map(parse_execution_status)
            .transpose()?;
        let from = self
            .from_
            .as_deref()
            .map(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| {
                        PyValueError::new_err(format!("invalid from_ datetime {s:?}: {e}"))
                    })
            })
            .transpose()?;
        let to = self
            .to
            .as_deref()
            .map(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| PyValueError::new_err(format!("invalid to datetime {s:?}: {e}")))
            })
            .transpose()?;
        let sort_by = parse_sort_by(&self.sort_by)?;
        let sort_order = parse_sort_order(&self.sort_order)?;
        Ok(zart::ListExecutionsParams {
            status,
            task_name: self.task_name.clone(),
            from,
            to,
            search: self.search.clone(),
            sort_by,
            sort_order,
            limit: self.limit,
            offset: self.offset,
        })
    }
}

#[pymethods]
impl PyListExecutionsParams {
    #[new]
    #[pyo3(signature = (
        status = None,
        task_name = None,
        from_ = None,
        to = None,
        search = None,
        sort_by = "scheduled_at".to_string(),
        sort_order = "desc".to_string(),
        limit = 20,
        offset = 0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        status: Option<String>,
        task_name: Option<String>,
        from_: Option<&Bound<'_, PyAny>>,
        to: Option<&Bound<'_, PyAny>>,
        search: Option<String>,
        sort_by: String,
        sort_order: String,
        limit: usize,
        offset: usize,
    ) -> PyResult<Self> {
        if let Some(ref s) = status {
            parse_execution_status(s)?;
        }
        let from_iso = parse_optional_datetime(from_, "from_")?;
        let to_iso = parse_optional_datetime(to, "to")?;
        parse_sort_by(&sort_by)?;
        parse_sort_order(&sort_order)?;
        Ok(Self {
            status,
            task_name,
            from_: from_iso,
            to: to_iso,
            search,
            sort_by,
            sort_order,
            limit,
            offset,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ListExecutionsParams(status={:?}, task_name={:?}, sort_by={:?}, sort_order={:?}, limit={}, offset={})",
            self.status, self.task_name, self.sort_by, self.sort_order, self.limit, self.offset
        )
    }
}

/// Execution count statistics grouped by status.
#[pyclass(name = "ExecutionStats", get_all, frozen)]
pub struct PyExecutionStats {
    pub scheduled: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

impl From<zart::ExecutionStats> for PyExecutionStats {
    fn from(s: zart::ExecutionStats) -> Self {
        Self {
            scheduled: s.scheduled,
            running: s.running,
            completed: s.completed,
            failed: s.failed,
            cancelled: s.cancelled,
        }
    }
}

#[pymethods]
impl PyExecutionStats {
    fn __repr__(&self) -> String {
        format!(
            "ExecutionStats(scheduled={}, running={}, completed={}, failed={}, cancelled={})",
            self.scheduled, self.running, self.completed, self.failed, self.cancelled
        )
    }
}

/// A single run of a durable execution.
#[pyclass(name = "ExecutionRunRecord", get_all, frozen)]
pub struct PyExecutionRunRecord {
    pub run_id: String,
    pub execution_id: String,
    pub run_index: i32,
    pub payload: Py<PyAny>,
    pub status: String,
    pub result: Option<Py<PyAny>>,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub trigger: String,
}

impl PyExecutionRunRecord {
    fn from_rust_py(py: Python<'_>, run: zart::ExecutionRunRecord) -> PyResult<Self> {
        let payload = json_to_py(py, &run.payload)?.unbind();
        let result = match run.result {
            Some(ref val) => Some(json_to_py(py, val)?.unbind()),
            None => None,
        };
        Ok(Self {
            run_id: run.run_id,
            execution_id: run.execution_id,
            run_index: run.run_index,
            payload,
            status: run.status.to_string(),
            result,
            started_at: run.started_at.to_rfc3339(),
            completed_at: run.completed_at.map(|dt| dt.to_rfc3339()),
            trigger: execution_trigger_str(&run.trigger).to_string(),
        })
    }
}

#[pymethods]
impl PyExecutionRunRecord {
    fn __repr__(&self) -> String {
        format!(
            "ExecutionRunRecord(run_id={:?}, execution_id={:?}, run_index={}, status={:?}, trigger={:?})",
            self.run_id, self.execution_id, self.run_index, self.status, self.trigger
        )
    }
}

/// A single attempt record for a durable step.
#[pyclass(name = "StepAttemptRow", get_all, frozen)]
pub struct PyStepAttemptRow {
    pub attempt_id: String,
    pub step_id: String,
    pub attempt_number: i32,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub status: String,
    pub result: Option<Py<PyAny>>,
    pub error: Option<String>,
}

impl PyStepAttemptRow {
    fn from_rust_py(py: Python<'_>, attempt: zart_core::types::StepAttemptRow) -> PyResult<Self> {
        let result = match attempt.result {
            Some(ref val) => Some(json_to_py(py, val)?.unbind()),
            None => None,
        };
        Ok(Self {
            attempt_id: attempt.attempt_id,
            step_id: attempt.step_id,
            attempt_number: attempt.attempt_number,
            started_at: attempt.started_at.to_rfc3339(),
            completed_at: attempt.completed_at.map(|dt| dt.to_rfc3339()),
            status: step_attempt_status_str(&attempt.status).to_string(),
            result,
            error: attempt.error,
        })
    }
}

#[pymethods]
impl PyStepAttemptRow {
    fn __repr__(&self) -> String {
        format!(
            "StepAttemptRow(attempt_id={:?}, step_id={:?}, attempt_number={}, status={:?})",
            self.attempt_id, self.step_id, self.attempt_number, self.status
        )
    }
}

/// A step record from a durable execution run.
#[pyclass(name = "StepRow", get_all, frozen)]
pub struct PyStepRow {
    pub step_id: String,
    pub run_id: String,
    pub step_name: String,
    pub step_kind: String,
    pub task_id: Option<String>,
    pub status: String,
    pub retry_attempt: i32,
    pub retry_config: Option<Py<PyAny>>,
    pub result: Option<Py<PyAny>>,
    pub last_error: Option<String>,
    pub wg_total: Option<i32>,
    pub wg_remaining: Option<i32>,
    pub wg_threshold: Option<i32>,
    pub wg_first_failed: Option<bool>,
    pub scheduled_at: String,
    pub completed_at: Option<String>,
}

impl PyStepRow {
    fn from_rust_py(py: Python<'_>, step: zart_core::types::StepRow) -> PyResult<Self> {
        let retry_config = match step.retry_config {
            Some(ref val) => Some(json_to_py(py, val)?.unbind()),
            None => None,
        };
        let result = match step.result {
            Some(ref val) => Some(json_to_py(py, val)?.unbind()),
            None => None,
        };
        Ok(Self {
            step_id: step.step_id,
            run_id: step.run_id,
            step_name: step.step_name,
            step_kind: step_kind_str(&step.step_kind).to_string(),
            task_id: step.task_id,
            status: step_status_str(&step.status).to_string(),
            retry_attempt: step.retry_attempt,
            retry_config,
            result,
            last_error: step.last_error,
            wg_total: step.wg_total,
            wg_remaining: step.wg_remaining,
            wg_threshold: step.wg_threshold,
            wg_first_failed: step.wg_first_failed,
            scheduled_at: step.scheduled_at.to_rfc3339(),
            completed_at: step.completed_at.map(|dt| dt.to_rfc3339()),
        })
    }
}

#[pymethods]
impl PyStepRow {
    fn __repr__(&self) -> String {
        format!(
            "StepRow(step_id={:?}, step_name={:?}, step_kind={:?}, status={:?})",
            self.step_id, self.step_name, self.step_kind, self.status
        )
    }
}

/// A step with its attempt history and retryability flag.
#[pyclass(name = "StepWithAttempts", get_all, frozen)]
pub struct PyStepWithAttempts {
    pub step: Py<PyStepRow>,
    pub attempts: Vec<Py<PyStepAttemptRow>>,
    pub retryable: bool,
}

impl PyStepWithAttempts {
    fn from_rust_py(py: Python<'_>, swa: zart::admin::StepWithAttempts) -> PyResult<Self> {
        let step = Py::new(py, PyStepRow::from_rust_py(py, swa.step)?)?;
        let attempts = swa
            .attempts
            .into_iter()
            .map(|a| Py::new(py, PyStepAttemptRow::from_rust_py(py, a)?))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            step,
            attempts,
            retryable: swa.retryable,
        })
    }
}

#[pymethods]
impl PyStepWithAttempts {
    fn __repr__(&self, py: Python<'_>) -> String {
        let step_name = &self.step.borrow(py).step_name;
        format!(
            "StepWithAttempts(step_name={:?}, attempts={}, retryable={})",
            step_name,
            self.attempts.len(),
            self.retryable
        )
    }
}

/// Full execution detail including the execution record, all runs, and steps with attempts.
#[pyclass(name = "ExecutionDetail", get_all, frozen)]
pub struct PyExecutionDetail {
    pub execution: Py<PyExecutionRecord>,
    pub runs: Vec<Py<PyExecutionRunRecord>>,
    pub steps: Vec<Py<PyStepWithAttempts>>,
}

impl PyExecutionDetail {
    fn from_rust(detail: zart::admin::ExecutionDetail) -> PyResult<Self> {
        Python::attach(|py| {
            let execution = Py::new(py, PyExecutionRecord::from_rust_py(py, detail.execution)?)?;
            let runs = detail
                .runs
                .into_iter()
                .map(|r| Py::new(py, PyExecutionRunRecord::from_rust_py(py, r)?))
                .collect::<PyResult<Vec<_>>>()?;
            let steps = detail
                .steps
                .into_iter()
                .map(|s| Py::new(py, PyStepWithAttempts::from_rust_py(py, s)?))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Self {
                execution,
                runs,
                steps,
            })
        })
    }
}

#[pymethods]
impl PyExecutionDetail {
    fn __repr__(&self, py: Python<'_>) -> String {
        let execution_id = &self.execution.borrow(py).execution_id;
        format!(
            "ExecutionDetail(execution_id={:?}, runs={}, steps={})",
            execution_id,
            self.runs.len(),
            self.steps.len()
        )
    }
}

/// Specification for which steps to rerun vs preserve in `rerun_steps`.
#[pyclass(name = "RerunSpec", get_all, frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRerunSpec {
    pub force_rerun: Vec<String>,
    pub preserve: Vec<String>,
    pub triggered_by: Option<String>,
}

impl From<&PyRerunSpec> for zart::RerunSpec {
    fn from(spec: &PyRerunSpec) -> Self {
        Self {
            force_rerun: spec.force_rerun.clone(),
            preserve: spec.preserve.clone(),
            triggered_by: spec.triggered_by.clone(),
        }
    }
}

#[pymethods]
impl PyRerunSpec {
    #[new]
    #[pyo3(signature = (force_rerun = vec![], preserve = vec![], triggered_by = None))]
    fn new(force_rerun: Vec<String>, preserve: Vec<String>, triggered_by: Option<String>) -> Self {
        Self {
            force_rerun,
            preserve,
            triggered_by,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RerunSpec(force_rerun={:?}, preserve={:?}, triggered_by={:?})",
            self.force_rerun, self.preserve, self.triggered_by
        )
    }
}

/// Advisory warning about a preserved step that may depend on rerun steps.
#[pyclass(name = "PotentiallyStaleDep", get_all, frozen)]
#[derive(Debug, Clone)]
pub struct PyPotentiallyStaleDep {
    pub preserved_step: String,
    pub possibly_depends_on: Vec<String>,
}

impl From<zart::PotentiallyStaleDep> for PyPotentiallyStaleDep {
    fn from(dep: zart::PotentiallyStaleDep) -> Self {
        Self {
            preserved_step: dep.preserved_step,
            possibly_depends_on: dep.possibly_depends_on,
        }
    }
}

#[pymethods]
impl PyPotentiallyStaleDep {
    fn __repr__(&self) -> String {
        format!(
            "PotentiallyStaleDep(preserved_step={:?}, possibly_depends_on={:?})",
            self.preserved_step, self.possibly_depends_on
        )
    }
}

/// Result of a selective `rerun_steps` operation.
#[pyclass(name = "RerunResult", get_all, frozen)]
pub struct PyRerunResult {
    pub new_run_number: u32,
    pub effective_rerun: Vec<String>,
    pub potentially_stale: Vec<Py<PyPotentiallyStaleDep>>,
}

impl PyRerunResult {
    fn from_rust(res: zart::RerunResult) -> PyResult<Self> {
        Python::attach(|py| {
            let potentially_stale = res
                .potentially_stale
                .into_iter()
                .map(|d| Py::new(py, PyPotentiallyStaleDep::from(d)))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Self {
                new_run_number: res.new_run_number,
                effective_rerun: res.effective_rerun,
                potentially_stale,
            })
        })
    }
}

#[pymethods]
impl PyRerunResult {
    fn __repr__(&self, py: Python<'_>) -> String {
        let _ = py;
        format!(
            "RerunResult(new_run_number={}, effective_rerun={:?}, potentially_stale={})",
            self.new_run_number,
            self.effective_rerun,
            self.potentially_stale.len()
        )
    }
}

/// Scope of a pause or resume operation.
#[pyclass(name = "PauseScope", get_all, frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPauseScope {
    pub execution_id: Option<String>,
    pub task_name: Option<String>,
    pub step_pattern: Option<String>,
    pub expires_at: Option<String>,
    pub triggered_by: Option<String>,
    pub reason: Option<String>,
}

impl PyPauseScope {
    fn to_rust(&self) -> PyResult<zart::PauseScope> {
        let expires_at = self
            .expires_at
            .as_deref()
            .map(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| {
                        PyValueError::new_err(format!("invalid expires_at datetime {s:?}: {e}"))
                    })
            })
            .transpose()?;
        Ok(zart::PauseScope {
            execution_id: self.execution_id.clone(),
            task_name: self.task_name.clone(),
            step_pattern: self.step_pattern.clone(),
            expires_at,
            triggered_by: self.triggered_by.clone(),
            reason: self.reason.clone(),
        })
    }

    fn from_rust(scope: zart::PauseScope) -> Self {
        Self {
            execution_id: scope.execution_id,
            task_name: scope.task_name,
            step_pattern: scope.step_pattern,
            expires_at: scope.expires_at.map(|dt| dt.to_rfc3339()),
            triggered_by: scope.triggered_by,
            reason: scope.reason,
        }
    }
}

#[pymethods]
impl PyPauseScope {
    #[new]
    #[pyo3(signature = (
        execution_id = None,
        task_name = None,
        step_pattern = None,
        expires_at = None,
        triggered_by = None,
        reason = None,
    ))]
    fn new(
        execution_id: Option<String>,
        task_name: Option<String>,
        step_pattern: Option<String>,
        expires_at: Option<&Bound<'_, PyAny>>,
        triggered_by: Option<String>,
        reason: Option<String>,
    ) -> PyResult<Self> {
        let expires_iso = parse_optional_datetime(expires_at, "expires_at")?;
        Ok(Self {
            execution_id,
            task_name,
            step_pattern,
            expires_at: expires_iso,
            triggered_by,
            reason,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "PauseScope(execution_id={:?}, task_name={:?}, step_pattern={:?})",
            self.execution_id, self.task_name, self.step_pattern
        )
    }
}

/// A pause rule stored in the database.
#[pyclass(name = "PauseRule", get_all, frozen)]
pub struct PyPauseRule {
    pub rule_id: String,
    pub scope: Py<PyPauseScope>,
    pub created_at: String,
    pub deleted_at: Option<String>,
    pub reason: Option<String>,
}

impl PyPauseRule {
    fn from_rust_py(py: Python<'_>, rule: zart::PauseRule) -> PyResult<Self> {
        let scope = Py::new(py, PyPauseScope::from_rust(rule.scope))?;
        Ok(Self {
            rule_id: rule.rule_id,
            scope,
            created_at: rule.created_at.to_rfc3339(),
            deleted_at: rule.deleted_at.map(|dt| dt.to_rfc3339()),
            reason: rule.reason,
        })
    }

    fn from_rust(rule: zart::PauseRule) -> PyResult<Self> {
        Python::attach(|py| Self::from_rust_py(py, rule))
    }
}

#[pymethods]
impl PyPauseRule {
    fn __repr__(&self) -> String {
        format!(
            "PauseRule(rule_id={:?}, created_at={:?}, deleted_at={:?})",
            self.rule_id, self.created_at, self.deleted_at
        )
    }
}

/// Result of a resume operation.
#[pyclass(name = "ResumeResult", get_all, frozen)]
#[derive(Debug, Clone)]
pub struct PyResumeResult {
    pub rules_deleted: usize,
}

impl From<zart::ResumeResult> for PyResumeResult {
    fn from(res: zart::ResumeResult) -> Self {
        Self {
            rules_deleted: res.rules_deleted,
        }
    }
}

#[pymethods]
impl PyResumeResult {
    fn __repr__(&self) -> String {
        format!("ResumeResult(rules_deleted={})", self.rules_deleted)
    }
}

/// Filter for listing pause rules.
#[pyclass(name = "PauseRuleFilter", get_all, frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPauseRuleFilter {
    pub execution_id: Option<String>,
    pub task_name: Option<String>,
    pub include_deleted: bool,
}

impl From<&PyPauseRuleFilter> for zart::PauseRuleFilter {
    fn from(f: &PyPauseRuleFilter) -> Self {
        Self {
            execution_id: f.execution_id.clone(),
            task_name: f.task_name.clone(),
            include_deleted: f.include_deleted,
        }
    }
}

#[pymethods]
impl PyPauseRuleFilter {
    #[new]
    #[pyo3(signature = (execution_id = None, task_name = None, include_deleted = false))]
    fn new(execution_id: Option<String>, task_name: Option<String>, include_deleted: bool) -> Self {
        Self {
            execution_id,
            task_name,
            include_deleted,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PauseRuleFilter(execution_id={:?}, task_name={:?}, include_deleted={})",
            self.execution_id, self.task_name, self.include_deleted
        )
    }
}

/// Control-plane client for starting, querying, and awaiting durable executions.
#[pyclass(name = "DurableScheduler", frozen)]
pub struct PyDurableScheduler {
    inner: Arc<zart::DurableScheduler>,
}

#[pymethods]
impl PyDurableScheduler {
    /// Create a `DurableScheduler` from a `PgBackend`.
    #[new]
    fn new(backend: &PyPgBackend) -> Self {
        let sched = zart::DurableScheduler::from_backend(&*backend.inner);
        Self {
            inner: Arc::new(sched),
        }
    }

    /// Start a new durable execution with a JSON-compatible payload.
    fn start<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        task_name: String,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = py_to_json(payload)?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = sched
                .start(&execution_id, &task_name, payload_json)
                .await
                .map_err(scheduler_err_to_py)?;
            Ok(PyScheduleResult {
                task_id: res.task_id,
                execution_time: res.execution_time.to_rfc3339(),
            })
        })
    }

    /// Fetch the current status and metadata of a durable execution.
    fn status<'py>(&self, py: Python<'py>, execution_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let record = sched
                .status(&execution_id)
                .await
                .map_err(scheduler_err_to_py)?;
            PyExecutionRecord::from_rust(record)
        })
    }

    /// Wait until the execution reaches a terminal state and return its `ExecutionRecord`.
    #[pyo3(signature = (execution_id, timeout_secs = 30.0, poll_interval_secs = None))]
    fn wait<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        timeout_secs: f64,
        poll_interval_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_from_secs(timeout_secs, "timeout_secs")?;
        let poll_interval = poll_interval_secs
            .map(|s| duration_from_secs(s, "poll_interval_secs"))
            .transpose()?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let record = sched
                .wait(&execution_id, timeout, poll_interval)
                .await
                .map_err(scheduler_err_to_py)?;
            PyExecutionRecord::from_rust(record)
        })
    }

    /// Wait until the execution completes and return its deserialized JSON result.
    #[pyo3(signature = (execution_id, timeout_secs = 30.0, poll_interval_secs = None))]
    fn wait_completion<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        timeout_secs: f64,
        poll_interval_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_from_secs(timeout_secs, "timeout_secs")?;
        let poll_interval = poll_interval_secs
            .map(|s| duration_from_secs(s, "poll_interval_secs"))
            .transpose()?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value: serde_json::Value = sched
                .wait_completion(&execution_id, timeout, poll_interval)
                .await
                .map_err(scheduler_err_to_py)?;
            Python::attach(|py| Ok(json_to_py(py, &value)?.unbind()))
        })
    }

    /// Cancel a running or scheduled durable execution.
    fn cancel<'py>(&self, py: Python<'py>, execution_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .cancel(&execution_id)
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Deliver an external event payload to a waiting durable execution.
    fn offer_event<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        event_name: String,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = py_to_json(payload)?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .offer_event(&execution_id, &event_name, payload_json)
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Stop and delete a recurring durable task schedule by its `task_id`.
    fn stop_recurring<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .stop_recurring(&task_id)
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Start a new durable execution inside a caller-owned `ClientTrx` transaction.
    fn start_in_tx<'py>(
        &self,
        py: Python<'py>,
        tx: &PyClientTrx,
        execution_id: String,
        task_name: String,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = py_to_json(payload)?;
        let sched = self.inner.clone();
        let tx_inner = tx.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx_inner.lock().await;
            let pg_tx = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("Transaction is already closed"))?;
            let res = sched
                .start_in_tx(pg_tx, &execution_id, &task_name, payload_json)
                .await
                .map_err(scheduler_err_to_py)?;
            Ok(PyScheduleResult {
                task_id: res.task_id,
                execution_time: res.execution_time.to_rfc3339(),
            })
        })
    }

    /// List durable execution records with optional filters and pagination.
    #[pyo3(signature = (params = None))]
    fn list_executions<'py>(
        &self,
        py: Python<'py>,
        params: Option<PyListExecutionsParams>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = match params {
            Some(p) => p.to_rust()?,
            None => zart::ListExecutionsParams::default(),
        };
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let records = sched
                .list_executions(rust_params)
                .await
                .map_err(scheduler_err_to_py)?;
            Python::attach(|py| {
                records
                    .into_iter()
                    .map(|r| PyExecutionRecord::from_rust_py(py, r))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Return aggregate execution counts grouped by status.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = sched.stats().await.map_err(scheduler_err_to_py)?;
            Ok(PyExecutionStats::from(stats))
        })
    }

    /// Return the current `run_id` for a durable execution, or `None` if never started.
    fn get_current_run_id<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .get_current_run_id(&execution_id)
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// List all runs for a durable execution.
    fn list_runs<'py>(&self, py: Python<'py>, execution_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let runs = sched
                .list_runs(&execution_id)
                .await
                .map_err(scheduler_err_to_py)?;
            Python::attach(|py| {
                runs.into_iter()
                    .map(|r| PyExecutionRunRecord::from_rust_py(py, r))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Return full execution detail (execution record, runs, and steps with attempts).
    #[pyo3(signature = (execution_id, run_id = None))]
    fn execution_detail<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        run_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let detail = sched
                .execution_detail(&execution_id, run_id.as_deref())
                .await
                .map_err(scheduler_err_to_py)?;
            PyExecutionDetail::from_rust(detail)
        })
    }

    /// Retry a single dead step within the given run and return the new task ID.
    #[pyo3(signature = (run_id, step_name, triggered_by = None))]
    fn retry_step<'py>(
        &self,
        py: Python<'py>,
        run_id: String,
        step_name: String,
        triggered_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .retry_step(&run_id, &step_name, triggered_by.as_deref())
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Retry a single dead step within the current run of `execution_id` and return the new task ID.
    #[pyo3(signature = (execution_id, step_name, triggered_by = None))]
    fn retry_step_current_run<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        step_name: String,
        triggered_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .retry_step_current_run(&execution_id, &step_name, triggered_by.as_deref())
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Restart an entire execution from scratch, optionally replacing its payload, and return the new `run_id`.
    #[pyo3(signature = (execution_id, new_payload = None, triggered_by = None))]
    fn restart<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        new_payload: Option<&Bound<'py, PyAny>>,
        triggered_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = match new_payload {
            Some(obj) if !obj.is_none() => Some(py_to_json(obj)?),
            _ => None,
        };
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .restart(&execution_id, payload_json, triggered_by.as_deref())
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Selectively rerun a subset of steps while preserving others.
    #[pyo3(signature = (execution_id, spec = None))]
    fn rerun_steps<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        spec: Option<PyRerunSpec>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_spec = spec.as_ref().map(zart::RerunSpec::from).unwrap_or_default();
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = sched
                .rerun_steps(&execution_id, rust_spec)
                .await
                .map_err(scheduler_err_to_py)?;
            PyRerunResult::from_rust(res)
        })
    }

    /// Create a pause rule for the given scope.
    fn pause<'py>(&self, py: Python<'py>, scope: PyPauseScope) -> PyResult<Bound<'py, PyAny>> {
        let rust_scope = scope.to_rust()?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rule = sched.pause(rust_scope).await.map_err(scheduler_err_to_py)?;
            PyPauseRule::from_rust(rule)
        })
    }

    /// Resume execution by soft-deleting matching pause rules.
    #[pyo3(signature = (scope, deleted_by = None))]
    fn resume<'py>(
        &self,
        py: Python<'py>,
        scope: PyPauseScope,
        deleted_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_scope = scope.to_rust()?;
        if deleted_by.is_some() {
            rust_scope.triggered_by = deleted_by;
        }
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = sched
                .resume(rust_scope)
                .await
                .map_err(scheduler_err_to_py)?;
            Ok(PyResumeResult::from(res))
        })
    }

    /// Resume by soft-deleting a specific pause rule by ID.
    #[pyo3(signature = (rule_id, deleted_by = None))]
    fn resume_rule_by_id<'py>(
        &self,
        py: Python<'py>,
        rule_id: String,
        deleted_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let deleted = sched
                .resume_rule_by_id(&rule_id, deleted_by.as_deref())
                .await
                .map_err(scheduler_err_to_py)?;
            Ok(PyResumeResult {
                rules_deleted: usize::from(deleted),
            })
        })
    }

    /// List pause rules matching the optional filter.
    #[pyo3(signature = (filter = None))]
    fn list_pause_rules<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyPauseRuleFilter>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_filter = filter.as_ref().map(zart::PauseRuleFilter::from);
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rules = sched
                .list_pause_rules(rust_filter)
                .await
                .map_err(scheduler_err_to_py)?;
            Python::attach(|py| {
                rules
                    .into_iter()
                    .map(|r| PyPauseRule::from_rust_py(py, r))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }
}
