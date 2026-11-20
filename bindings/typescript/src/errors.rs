//! Structured error translation. Native errors are thrown as `napi::Error` whose
//! `reason` is a JSON string: `{"_tag": "...", "message": "..."}` so the TS layer
//! can map them to tagged Effect errors. Tags: ZartBackendError, ZartSchemaError,
//! ExecutionNotFound, ExecutionAlreadyExists, WaitTimedOut, ZartError.

use napi::bindgen_prelude::Status;
use napi_derive::napi;
use serde::{Deserialize, Serialize};

/// Serializable error raised inside a step (persisted so BusinessErr survives replay).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsStepError {
    pub error_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl std::fmt::Display for TsStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}
impl std::error::Error for TsStepError {}

/// Result of a step body: `kind` is `"ok" | "business_err" | "zart_err"`.
#[napi(object)]
#[derive(Debug, Clone)]
pub struct StepOutcome {
    pub kind: String,
    /// JSON value for `ok`.
    pub value: Option<serde_json::Value>,
    /// Business/zart error details.
    pub error_type: Option<String>,
    pub message: Option<String>,
    pub data: Option<serde_json::Value>,
}

impl StepOutcome {
    pub fn ok(value: serde_json::Value) -> Self {
        Self {
            kind: "ok".into(),
            value: Some(value),
            error_type: None,
            message: None,
            data: None,
        }
    }
    pub fn business_err(e: TsStepError) -> Self {
        Self {
            kind: "business_err".into(),
            value: None,
            error_type: Some(e.error_type),
            message: Some(e.message),
            data: e.data,
        }
    }
    pub fn zart_err(tag: &str, message: impl Into<String>) -> Self {
        Self {
            kind: "zart_err".into(),
            value: None,
            error_type: Some(tag.into()),
            message: Some(message.into()),
            data: None,
        }
    }
}

fn tagged(tag: &str, message: String, status: Status) -> napi::Error {
    let reason = serde_json::json!({ "_tag": tag, "message": message }).to_string();
    napi::Error::new(status, reason)
}

pub fn backend_err(context: &str, e: impl std::fmt::Display) -> napi::Error {
    tagged(
        "ZartBackendError",
        format!("{context}: {e}"),
        Status::GenericFailure,
    )
}

pub fn schema_err(e: impl std::fmt::Display) -> napi::Error {
    tagged("ZartSchemaError", e.to_string(), Status::InvalidArg)
}

pub fn scheduler_err(err: zart::SchedulerError) -> napi::Error {
    use zart::SchedulerError as E;
    match err {
        E::WaitTimedOut(m) => tagged(
            "WaitTimedOut",
            format!("Timed out waiting for execution '{m}'"),
            Status::GenericFailure,
        ),
        E::ExecutionNotFound(m) => tagged(
            "ExecutionNotFound",
            format!("Execution '{m}' not found"),
            Status::GenericFailure,
        ),
        e @ E::ExecutionAlreadyExists(..) => tagged(
            "ExecutionAlreadyExists",
            e.to_string(),
            Status::GenericFailure,
        ),
        e @ (E::Serialization(_) | E::Deserialization(_)) => schema_err(e),
        e @ E::Database(_) => tagged("ZartBackendError", e.to_string(), Status::GenericFailure),
        e => tagged("ZartError", e.to_string(), Status::GenericFailure),
    }
}

/// Control-flow suspension signal (replay stop). JS must rethrow it unchanged from the
/// durable handler so the native boundary can intercept it.
pub fn control_flow_err(kind: &str) -> napi::Error {
    let reason = serde_json::json!({ "_tag": "ControlFlowSignal", "kind": kind }).to_string();
    napi::Error::new(Status::GenericFailure, reason)
}

/// Returns true if a JS rejection reason is a serialized `ControlFlowSignal`.
pub fn is_control_flow_reason(reason: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(reason)
        .ok()
        .and_then(|v| {
            v.get("_tag")
                .and_then(|t| t.as_str())
                .map(|t| t == "ControlFlowSignal")
        })
        .unwrap_or(false)
}

/// Extract a human message from a JS rejection reason (JSON `{message}` or raw text).
pub fn reason_message(reason: &str) -> String {
    serde_json::from_str::<serde_json::Value>(reason)
        .ok()
        .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(str::to_owned))
        .unwrap_or_else(|| reason.to_owned())
}

fn is_cf(e: &zart::StepError) -> Option<&'static str> {
    match e {
        zart::StepError::Scheduled { .. } => Some("scheduled"),
        zart::StepError::StepExecuted { .. } => Some("step_executed"),
        _ => None,
    }
}

/// Translate a `StepError` into a rejection; control-flow variants are parked in `cf_slot`.
pub fn step_err_to_napi(
    err: zart::StepError,
    cf_slot: &std::sync::Mutex<Option<zart::StepError>>,
) -> napi::Error {
    if let Some(kind) = is_cf(&err) {
        if let Ok(mut g) = cf_slot.lock() {
            *g = Some(err);
        }
        return control_flow_err(kind);
    }
    let (tag, extra) = step_error_tag(&err);
    let mut v = serde_json::json!({ "_tag": tag, "message": err.to_string() });
    if let (Some(o), serde_json::Value::Object(x)) = (v.as_object_mut(), extra) {
        o.extend(x);
    }
    napi::Error::new(Status::GenericFailure, v.to_string())
}

pub fn task_err_to_napi(
    err: zart::TaskError,
    cf_slot: &std::sync::Mutex<Option<zart::StepError>>,
) -> napi::Error {
    match err {
        zart::TaskError::StepFailed { source, .. } => step_err_to_napi(source, cf_slot),
        other => tagged("StepFailedError", other.to_string(), Status::GenericFailure),
    }
}

/// Tag + extra fields describing a non-control-flow `StepError`.
pub fn step_error_tag(err: &zart::StepError) -> (&'static str, serde_json::Value) {
    use zart::StepError as E;
    match err {
        E::RetryExhausted { step, attempts } => (
            "RetryExhaustedError",
            serde_json::json!({ "stepName": step, "attempts": attempts }),
        ),
        E::Timeout { step, duration } => (
            "StepTimeoutError",
            serde_json::json!({ "stepName": step, "timeoutMs": duration.as_millis() as u64 }),
        ),
        E::DeadlineExceeded { step } => (
            "DeadlineExceededError",
            serde_json::json!({ "stepName": step }),
        ),
        E::Failed { step, reason } => (
            "StepFailedError",
            serde_json::json!({ "stepName": step, "reason": reason }),
        ),
        _ => ("StepFailedError", serde_json::json!({})),
    }
}

impl StepOutcome {
    /// Outcome for a non-control-flow `StepError` surfaced as `zart_err`.
    pub fn from_step_error(err: &zart::StepError) -> Self {
        let (tag, extra) = step_error_tag(err);
        Self::zart_err_data(tag, err.to_string(), extra)
    }

    pub fn from_zart_step_error(err: &zart::ZartStepError) -> Self {
        use zart::ZartStepError as E;
        match err {
            E::RetryExhausted {
                step,
                attempts,
                last_error,
            } => Self::zart_err_data(
                "RetryExhaustedError",
                err.to_string(),
                serde_json::json!({ "stepName": step, "attempts": attempts, "lastError": last_error }),
            ),
            E::TimedOut { step, duration } => Self::zart_err_data(
                "StepTimeoutError",
                err.to_string(),
                serde_json::json!({ "stepName": step, "timeoutMs": duration.as_millis() as u64 }),
            ),
            E::DeadlineExceeded { step } => Self::zart_err_data(
                "DeadlineExceededError",
                err.to_string(),
                serde_json::json!({ "stepName": step }),
            ),
        }
    }

    fn zart_err_data(tag: &str, message: String, mut data: serde_json::Value) -> Self {
        if let Some(o) = data.as_object_mut() {
            o.insert("_tag".into(), tag.into());
            o.insert("message".into(), message.clone().into());
        }
        Self {
            kind: "zart_err".into(),
            value: None,
            error_type: Some(tag.into()),
            message: Some(message),
            data: Some(data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_reason_is_json() {
        let e = scheduler_err(zart::SchedulerError::ExecutionNotFound("x".into()));
        let v: serde_json::Value = serde_json::from_str(&e.reason).unwrap();
        assert_eq!(v["_tag"], "ExecutionNotFound");
    }

    #[test]
    fn outcome_kinds() {
        assert_eq!(StepOutcome::ok(serde_json::json!(1)).kind, "ok");
        let b = StepOutcome::business_err(TsStepError {
            error_type: "E".into(),
            message: "m".into(),
            data: None,
        });
        assert_eq!(b.kind, "business_err");
        assert_eq!(StepOutcome::zart_err("T", "m").kind, "zart_err");
    }
}
