//! Storage operations for pause/resume rules.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::StorageError;

// ── Types ──────────────────────────────────────────────────────────────────────

/// A pause rule stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseRule {
    /// Unique rule identifier.
    pub rule_id: String,
    /// Target a specific execution (None = all executions of task_name).
    pub execution_id: Option<String>,
    /// Target all executions of a given task name (None = only the specific execution_id).
    pub task_name: Option<String>,
    /// Glob pattern for step names (e.g. `"send-*"`). None = all steps.
    pub step_pattern: Option<String>,
    /// Optional human-readable reason for this pause (audit annotation).
    pub reason: Option<String>,
    /// When the rule was created.
    pub created_at: DateTime<Utc>,
    /// Optional auto-expiry for the pause rule.
    pub expires_at: Option<DateTime<Utc>>,
    /// Operator who created the rule.
    pub created_by: Option<String>,
    /// When the rule was soft-deleted (None = active).
    pub deleted_at: Option<DateTime<Utc>>,
    /// Operator who deleted the rule.
    pub deleted_by: Option<String>,
}

/// Filter for listing pause rules.
#[derive(Debug, Clone, Default)]
pub struct PauseRuleFilter {
    /// Filter by execution_id.
    pub execution_id: Option<String>,
    /// Filter by task_name.
    pub task_name: Option<String>,
    /// Whether to include soft-deleted rules.
    pub include_deleted: bool,
}

// ── Trait ──────────────────────────────────────────────────────────────────────

/// Storage operations for pause/resume rules.
///
/// Separate from `DurableStorage` and `TaskScheduler` — pause rules
/// are operational controls that happen to affect scheduling.
#[async_trait]
pub trait PauseStorage: Send + Sync {
    /// Create a new pause rule. Returns the created rule with its ID.
    async fn create_pause_rule(&self, rule: PauseRule) -> Result<PauseRule, StorageError> {
        let _ = rule;
        Err(StorageError::NotImplemented("create_pause_rule"))
    }

    /// Soft-delete a pause rule by ID. Returns `true` if a rule was found and deleted.
    async fn delete_pause_rule(
        &self,
        rule_id: &str,
        deleted_by: Option<&str>,
    ) -> Result<bool, StorageError> {
        let _ = (rule_id, deleted_by);
        Err(StorageError::NotImplemented("delete_pause_rule"))
    }

    /// List pause rules matching the filter.
    async fn list_pause_rules(
        &self,
        filter: PauseRuleFilter,
    ) -> Result<Vec<PauseRule>, StorageError> {
        let _ = filter;
        Err(StorageError::NotImplemented("list_pause_rules"))
    }

    /// Check if any active pause rule matches the given execution/step.
    ///
    /// Used at scheduling time — returns `true` if scheduling should be skipped.
    async fn is_paused(
        &self,
        execution_id: &str,
        task_name: &str,
        step_name: Option<&str>,
    ) -> Result<bool, StorageError> {
        let _ = (execution_id, task_name, step_name);
        Err(StorageError::NotImplemented("is_paused"))
    }
}

/// Type alias for the new name used in `zart::store`.
pub use PauseStorage as PauseStore;
