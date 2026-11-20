use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::duration_from_secs;

/// Policy controlling how overlapping occurrences of a recurring durable execution are handled.
#[pyclass(name = "OverlapPolicy", frozen, eq, eq_int, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyOverlapPolicy {
    /// Do nothing if a non-terminal execution from the previous occurrence is still running.
    SkipIfRunning,
    /// Cancel the previous running execution and start a new one.
    CancelAndRestart,
    /// Always start a new execution regardless of whether a previous one is running.
    AlwaysStart,
}

#[pymethods]
impl PyOverlapPolicy {
    #[allow(non_upper_case_globals)]
    #[classattr]
    const skip_if_running: Self = Self::SkipIfRunning;

    #[allow(non_upper_case_globals)]
    #[classattr]
    const cancel_and_restart: Self = Self::CancelAndRestart;

    #[allow(non_upper_case_globals)]
    #[classattr]
    const always_start: Self = Self::AlwaysStart;
}

impl From<PyOverlapPolicy> for zart::OverlapPolicy {
    fn from(policy: PyOverlapPolicy) -> Self {
        match policy {
            PyOverlapPolicy::SkipIfRunning => Self::SkipIfRunning,
            PyOverlapPolicy::CancelAndRestart => Self::CancelAndRestart,
            PyOverlapPolicy::AlwaysStart => Self::AlwaysStart,
        }
    }
}

/// Recurrence schedule for recurring durable executions.
#[pyclass(name = "Recurrence", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRecurrence {
    pub(crate) inner: zart_scheduler::Recurrence,
}

#[pymethods]
impl PyRecurrence {
    /// Create a fixed-delay recurrence from a `DurationLike` (`int`/`float` seconds,
    /// `datetime.timedelta`, or duration string such as `"500ms"` / `"5s"`).
    #[staticmethod]
    fn fixed_delay(py: Python<'_>, delay: &Bound<'_, PyAny>) -> PyResult<Self> {
        let decorators = py.import("zart.decorators")?;
        let secs_obj = decorators.call_method1("parse_duration", (delay,))?;
        let secs: f64 = secs_obj.extract()?;
        let dur = duration_from_secs(secs, "delay")?;
        let duration_ms = u64::try_from(dur.as_millis())
            .map_err(|_| PyValueError::new_err("delay is out of range for milliseconds"))?;
        Ok(Self {
            inner: zart_scheduler::Recurrence::FixedDelay { duration_ms },
        })
    }

    /// Create a cron recurrence from a 6-field cron expression and IANA timezone.
    #[staticmethod]
    #[pyo3(signature = (expression, timezone = "UTC".to_string()))]
    fn cron(expression: String, timezone: String) -> PyResult<Self> {
        if expression.trim().is_empty() {
            return Err(PyValueError::new_err("cron expression cannot be empty"));
        }
        if timezone.trim().is_empty() {
            return Err(PyValueError::new_err("timezone cannot be empty"));
        }
        Ok(Self {
            inner: zart_scheduler::Recurrence::Cron {
                expression,
                timezone,
            },
        })
    }

    /// Recurrence kind (`"fixed_delay"` or `"cron"`).
    #[getter]
    fn kind(&self) -> &'static str {
        match &self.inner {
            zart_scheduler::Recurrence::FixedDelay { .. } => "fixed_delay",
            zart_scheduler::Recurrence::Cron { .. } => "cron",
        }
    }

    /// Delay in milliseconds for `fixed_delay` recurrences, or `None` for `cron`.
    #[getter]
    fn duration_ms(&self) -> Option<u64> {
        match &self.inner {
            zart_scheduler::Recurrence::FixedDelay { duration_ms } => Some(*duration_ms),
            zart_scheduler::Recurrence::Cron { .. } => None,
        }
    }

    /// Cron expression for `cron` recurrences, or `None` for `fixed_delay`.
    #[getter]
    fn expression(&self) -> Option<&str> {
        match &self.inner {
            zart_scheduler::Recurrence::Cron { expression, .. } => Some(expression.as_str()),
            zart_scheduler::Recurrence::FixedDelay { .. } => None,
        }
    }

    /// IANA timezone for `cron` recurrences, or `None` for `fixed_delay`.
    #[getter]
    fn timezone(&self) -> Option<&str> {
        match &self.inner {
            zart_scheduler::Recurrence::Cron { timezone, .. } => Some(timezone.as_str()),
            zart_scheduler::Recurrence::FixedDelay { .. } => None,
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            zart_scheduler::Recurrence::FixedDelay { duration_ms } => {
                format!("Recurrence.fixed_delay(duration_ms={duration_ms})")
            }
            zart_scheduler::Recurrence::Cron {
                expression,
                timezone,
            } => format!("Recurrence.cron(expression={expression:?}, timezone={timezone:?})"),
        }
    }
}
