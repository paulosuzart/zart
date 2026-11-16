use std::fmt;

use pyo3::exceptions::{PyBaseException, PyException};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::serde_bridge::py_to_json;

pyo3::create_exception!(
    _zart,
    ZartControlFlowSignal,
    PyBaseException,
    "Internal control-flow signal for Zart step scheduling and replay. Inherits from BaseException."
);

pyo3::create_exception!(
    _zart,
    ZartError,
    PyException,
    "Base exception for Zart durable execution errors."
);

pyo3::create_exception!(
    _zart,
    ZartStepFailedError,
    ZartError,
    "Raised when a required step fails with a business or framework error."
);

pyo3::create_exception!(
    _zart,
    ZartRetryExhaustedError,
    ZartError,
    "Raised when a step exhausts all configured retry attempts."
);

pyo3::create_exception!(
    _zart,
    ZartStepTimeoutError,
    ZartError,
    "Raised when a step exceeds its configured timeout."
);

pyo3::create_exception!(
    _zart,
    ZartDeadlineExceededError,
    ZartError,
    "Raised when a step or event deadline is exceeded."
);

/// Serializable error representation for Python exceptions raised inside a step.
///
/// Persisted in PostgreSQL so `StepOutcome.BusinessErr` survives body replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyStepError {
    pub error_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl fmt::Display for PyStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

impl std::error::Error for PyStepError {}

impl PyStepError {
    /// Convert a Python exception into a serializable `PyStepError`.
    pub fn from_py_err(py: Python<'_>, err: PyErr) -> Self {
        let error_type = err
            .get_type(py)
            .qualname()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "Exception".to_string());
        let message = err.value(py).str().map(|s| s.to_string()).unwrap_or_default();
        let data = err
            .value(py)
            .getattr("data")
            .ok()
            .and_then(|val| py_to_json(&val).ok());

        Self {
            error_type,
            message,
            data,
        }
    }
}

/// Register all Zart exception classes on the `_zart` module.
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ZartControlFlowSignal", m.py().get_type::<ZartControlFlowSignal>())?;
    m.add("ZartError", m.py().get_type::<ZartError>())?;
    m.add("ZartStepFailedError", m.py().get_type::<ZartStepFailedError>())?;
    m.add(
        "ZartRetryExhaustedError",
        m.py().get_type::<ZartRetryExhaustedError>(),
    )?;
    m.add("ZartStepTimeoutError", m.py().get_type::<ZartStepTimeoutError>())?;
    m.add(
        "ZartDeadlineExceededError",
        m.py().get_type::<ZartDeadlineExceededError>(),
    )?;
    Ok(())
}
