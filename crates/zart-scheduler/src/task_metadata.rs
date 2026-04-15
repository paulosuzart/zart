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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn body_roundtrip() {
        let meta = TaskMetadata::body("exec-1:run:0", "exec-1");
        let v = meta.to_json_value();
        assert_eq!(v["mode"], "body");
        assert_eq!(v["run_id"], "exec-1:run:0");
        assert_eq!(v["execution_id"], "exec-1");
        assert_eq!(v.as_object().unwrap().len(), 3);

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn step_roundtrip_minimal() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "charge-card".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        let v = meta.to_json_value();
        assert_eq!(v["mode"], "step");
        assert_eq!(v["step_type"], "step");
        assert_eq!(v["run_id"], "exec-1:run:0");
        assert_eq!(v["execution_id"], "exec-1");
        assert_eq!(v["step_name"], "charge-card");
        assert_eq!(v["retry_attempt"], 0);
        assert_eq!(v["retry_attempt"], 0);
        assert!(v.get("retry_config").is_none());
        assert!(v.get("deadline").is_none());
        assert!(v.get("is_wait_all_child").is_none());
        assert!(v.get("wg_step_name").is_none());

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn step_roundtrip_full() {
        let rc = json!({ "max_attempts": 3, "delay_ms": 1000 });
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "charge-card".into(),
            retry_attempt: 2,
            retry_config: Some(rc.clone()),
            deadline: Some("2026-12-31T00:00:00Z".into()),
            is_wait_all_child: Some(true),
            wg_step_name: Some("__wg__all__abc".into()),
        };
        let v = meta.to_json_value();
        assert_eq!(v["retry_attempt"], 2);
        assert_eq!(v["retry_config"], rc);
        assert_eq!(v["deadline"], "2026-12-31T00:00:00Z");
        assert_eq!(v["is_wait_all_child"], true);
        assert_eq!(v["wg_step_name"], "__wg__all__abc");

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn step_type_sleep_roundtrip() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Sleep,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "__sleep".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        let v = meta.to_json_value();
        assert_eq!(v["step_type"], "sleep");

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn step_type_wait_for_event_roundtrip() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::WaitForEvent,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "approval".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        let v = meta.to_json_value();
        assert_eq!(v["step_type"], "wait_for_event");

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn wait_group_child_roundtrip() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "child-a".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: Some(true),
            wg_step_name: Some("__wg__all__abc".into()),
        };
        let v = meta.to_json_value();
        assert_eq!(v["is_wait_all_child"], true);
        assert_eq!(v["wg_step_name"], "__wg__all__abc");

        let back = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn from_json_value_defaults_missing_fields() {
        let v = json!({
            "mode": "step",
            "step_type": "step",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
            "step_name": "charge",
        });
        let meta = TaskMetadata::from_json_value(v).unwrap();
        assert_eq!(meta.retry_attempt(), 0);
        assert!(meta.retry_config().is_none());
        assert!(meta.deadline().is_none());
        assert!(!meta.is_wait_all_child());
        assert!(meta.wg_step_name().is_none());
    }

    #[test]
    fn from_json_value_returns_err_on_bad_input() {
        let v = json!("not an object");
        assert!(TaskMetadata::from_json_value(v).is_err());
    }

    #[test]
    fn accessors_on_body() {
        let meta = TaskMetadata::body("r", "e");
        assert_eq!(meta.execution_id(), "e");
        assert_eq!(meta.run_id(), "r");
        assert!(meta.step_name().is_none());
        assert!(meta.retry_config().is_none());
        assert_eq!(meta.retry_attempt(), 0);
        assert!(meta.deadline().is_none());
        assert!(!meta.is_step());
    }

    #[test]
    fn is_step_true_for_step_variant() {
        let meta = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "r".into(),
            execution_id: "e".into(),
            step_name: "s".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        };
        assert!(meta.is_step());
    }

    #[test]
    fn body_json_matches_legacy_format() {
        let legacy = json!({
            "mode": "body",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
        });
        let typed = TaskMetadata::body("exec-1:run:0", "exec-1").to_json_value();
        assert_eq!(legacy, typed);
    }

    #[test]
    fn step_json_matches_legacy_format() {
        let legacy = json!({
            "mode": "step",
            "step_type": "step",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
            "step_name": "charge-card",
            "retry_attempt": 0,
        });
        let typed = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "charge-card".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        }
        .to_json_value();
        assert_eq!(legacy, typed);
    }

    #[test]
    fn sleep_json_matches_legacy_format() {
        let legacy = json!({
            "mode": "step",
            "step_type": "sleep",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
            "step_name": "__sleep",
            "retry_attempt": 0,
        });
        let typed = TaskMetadata::Step {
            step_type: StepMetaType::Sleep,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "__sleep".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        }
        .to_json_value();
        assert_eq!(legacy, typed);
    }

    #[test]
    fn wait_for_event_json_matches_legacy_format() {
        let legacy = json!({
            "mode": "step",
            "step_type": "wait_for_event",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
            "step_name": "approval",
            "retry_attempt": 0,
        });
        let typed = TaskMetadata::Step {
            step_type: StepMetaType::WaitForEvent,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "approval".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: None,
            wg_step_name: None,
        }
        .to_json_value();
        assert_eq!(legacy, typed);
    }

    #[test]
    fn wait_group_child_json_matches_legacy_format() {
        let legacy = json!({
            "mode": "step",
            "step_type": "step",
            "run_id": "exec-1:run:0",
            "execution_id": "exec-1",
            "step_name": "child-a",
            "retry_attempt": 0,
            "is_wait_all_child": true,
            "wg_step_name": "__wg__all__abc",
        });
        let typed = TaskMetadata::Step {
            step_type: StepMetaType::Step,
            run_id: "exec-1:run:0".into(),
            execution_id: "exec-1".into(),
            step_name: "child-a".into(),
            retry_attempt: 0,
            retry_config: None,
            deadline: None,
            is_wait_all_child: Some(true),
            wg_step_name: Some("__wg__all__abc".into()),
        }
        .to_json_value();
        assert_eq!(legacy, typed);
    }
}
