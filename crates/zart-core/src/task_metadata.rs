//! Typed task metadata for the scheduler ↔ worker protocol.
//!
//! The `metadata` JSONB column on task rows carries internal routing information
//! (mode, run_id, execution_id, step_type, etc.). This module provides typed
//! structs that serialize to the same JSON shape, eliminating string-keyed access
//! and typo-prone `.get("…")` chains.

use serde::{Deserialize, Serialize};

/// Internal metadata carried on every task row.
///
/// Discriminated by the `"mode"` key so that serde's internally-tagged
/// representation produces `{"mode":"body",…}` / `{"mode":"step",…}` —
/// the exact wire format already stored in PostgreSQL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TaskMetadata {
    Body {
        run_id: String,
        execution_id: String,
    },
    Step {
        step_type: StepMetaType,
        run_id: String,
        execution_id: String,
        step_name: String,
        #[serde(default)]
        retry_attempt: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_config: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        is_wait_all_child: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wg_step_name: Option<String>,
    },
}

/// Step type discriminant stored in metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StepMetaType {
    Step,
    Sleep,
    WaitForEvent,
}

impl TaskMetadata {
    pub fn body(run_id: impl Into<String>, execution_id: impl Into<String>) -> Self {
        TaskMetadata::Body {
            run_id: run_id.into(),
            execution_id: execution_id.into(),
        }
    }

    pub fn execution_id(&self) -> &str {
        match self {
            TaskMetadata::Body { execution_id, .. } => execution_id,
            TaskMetadata::Step { execution_id, .. } => execution_id,
        }
    }

    pub fn run_id(&self) -> &str {
        match self {
            TaskMetadata::Body { run_id, .. } => run_id,
            TaskMetadata::Step { run_id, .. } => run_id,
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("TaskMetadata serialization is infallible")
    }

    /// Deserialize from the `serde_json::Value` stored in the DB.
    ///
    /// Returns `Err` if the value cannot be parsed as valid `TaskMetadata`.
    /// Callers (e.g. `dispatch_task`) should **fail the task** — never silently
    /// default to another variant, as that could re-execute user code in an
    /// unrecoverable state.
    pub fn from_json_value(v: serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(v)
    }

    pub fn deadline(&self) -> Option<&str> {
        match self {
            TaskMetadata::Step { deadline, .. } => deadline.as_deref(),
            _ => None,
        }
    }

    pub fn step_name(&self) -> Option<&str> {
        match self {
            TaskMetadata::Step { step_name, .. } => Some(step_name),
            _ => None,
        }
    }

    pub fn retry_config(&self) -> Option<&serde_json::Value> {
        match self {
            TaskMetadata::Step { retry_config, .. } => retry_config.as_ref(),
            _ => None,
        }
    }

    pub fn retry_attempt(&self) -> u32 {
        match self {
            TaskMetadata::Step { retry_attempt, .. } => *retry_attempt,
            _ => 0,
        }
    }

    pub fn step_type(&self) -> Option<&StepMetaType> {
        match self {
            TaskMetadata::Step { step_type, .. } => Some(step_type),
            _ => None,
        }
    }

    pub fn wg_step_name(&self) -> Option<&str> {
        match self {
            TaskMetadata::Step { wg_step_name, .. } => wg_step_name.as_deref(),
            _ => None,
        }
    }

    pub fn is_wait_all_child(&self) -> bool {
        match self {
            TaskMetadata::Step {
                is_wait_all_child, ..
            } => is_wait_all_child.unwrap_or(false),
            _ => false,
        }
    }

    pub fn is_step(&self) -> bool {
        matches!(self, TaskMetadata::Step { .. })
    }
}
