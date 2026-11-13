//! Step result and wait-group types used by `TaskContext` and completion hints.

use crate::error::StepError;

/// Discriminant for the kind of terminal outcome a step row holds.
///
/// Stored in `zart_steps.result_kind` and returned alongside the raw JSON
/// from body-mode lookup so the caller can construct the correct
/// [`StepOutcome`](crate::error::StepOutcome) variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultKind {
    /// Step succeeded — `result` holds `S::Output`.
    Ok,
    /// Step returned a business error — `result` holds `S::Error`.
    Err,
    /// Retries exhausted — `result` holds `S::Error` from the last attempt.
    RetryExhausted,
    /// Step timed out — `result` is NULL.
    TimedOut,
    /// wait_for_event deadline exceeded — `result` is NULL.
    DeadlineExceeded,
}

impl ResultKind {
    /// Parse from a database value (or default to `'ok'`).
    pub fn from_db(kind: Option<&str>) -> Self {
        match kind {
            Some("ok") => Self::Ok,
            Some("err") => Self::Err,
            Some("rx") => Self::RetryExhausted,
            Some("timeout") => Self::TimedOut,
            Some("dl") => Self::DeadlineExceeded,
            _ => Self::Ok, // backward compat: old rows without result_kind
        }
    }

    /// The DB string value to store.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
            Self::RetryExhausted => "rx",
            Self::TimedOut => "timeout",
            Self::DeadlineExceeded => "dl",
        }
    }
}

impl From<ResultKind> for zart_core::types::StepResultKind {
    fn from(k: ResultKind) -> Self {
        match k {
            ResultKind::Ok => Self::Ok,
            ResultKind::Err => Self::Err,
            ResultKind::RetryExhausted => Self::Rx,
            ResultKind::TimedOut => Self::Timeout,
            ResultKind::DeadlineExceeded => Self::Dl,
        }
    }
}

impl From<zart_core::types::StepResultKind> for ResultKind {
    fn from(k: zart_core::types::StepResultKind) -> Self {
        match k {
            zart_core::types::StepResultKind::Ok => Self::Ok,
            zart_core::types::StepResultKind::Err => Self::Err,
            zart_core::types::StepResultKind::Rx => Self::RetryExhausted,
            zart_core::types::StepResultKind::Timeout => Self::TimedOut,
            zart_core::types::StepResultKind::Dl => Self::DeadlineExceeded,
        }
    }
}

/// Wait-group semantics used by TaskContext wait APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitGroupKind {
    All,
    Any,
    FirstN(usize),
}

impl WaitGroupKind {
    /// Converts wait-group kind into a trigger threshold for `wg_remaining`.
    ///
    /// - all: trigger when remaining == 0
    /// - any: trigger when remaining == total - 1
    /// - first_n: trigger when remaining == total - n
    pub fn trigger_threshold(self, total: usize) -> Result<i32, StepError> {
        let total_i32 = i32::try_from(total).map_err(|_| StepError::Failed {
            step: "__wait_group".to_string(),
            reason: "too many wait-group handles to fit i32 threshold".to_string(),
        })?;

        match self {
            Self::All => Ok(0),
            Self::Any => {
                if total == 0 {
                    return Err(StepError::Failed {
                        step: "__wait_any".to_string(),
                        reason: "wait_any requires at least one handle".to_string(),
                    });
                }
                Ok(total_i32 - 1)
            }
            Self::FirstN(n) => {
                if n == 0 {
                    return Err(StepError::Failed {
                        step: "__wait_first_n".to_string(),
                        reason: "wait_first_n requires n > 0".to_string(),
                    });
                }
                if n > total {
                    return Err(StepError::Failed {
                        step: "__wait_first_n".to_string(),
                        reason: format!(
                            "wait_first_n requires n <= handles.len() (n={n}, len={total})"
                        ),
                    });
                }
                let n_i32 = i32::try_from(n).map_err(|_| StepError::Failed {
                    step: "__wait_first_n".to_string(),
                    reason: "n does not fit i32".to_string(),
                })?;
                Ok(total_i32 - n_i32)
            }
        }
    }
}
