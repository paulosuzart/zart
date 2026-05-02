//! Service layer for pause/resume operations.
//!
//! [`PauseService`] encapsulates all pause-rule management logic, extracted
//! from [`crate::durable::DurableScheduler`]. It holds a reference to
//! [`crate::store::StorageBackend`] (which includes `PauseStorage` as a supertrait)
//! and maps storage errors to [`SchedulerError`].

use std::sync::Arc;

use zart_core::store::pause_storage::{PauseRule, PauseRuleFilter};

use crate::admin::{PauseRule as DurablePauseRule, PauseScope, ResumeResult};
use crate::error::SchedulerError;
use crate::store::StorageBackend;

/// Manages pause rules for durable executions.
///
/// Held by [`crate::durable::DurableScheduler`] alongside [`super::ExecutionService`].
/// Pause is always enabled — `StorageBackend` includes `PauseStorage` as a supertrait.
pub struct PauseService {
    store: Arc<dyn StorageBackend>,
}

impl PauseService {
    /// Create a `PauseService` backed by the given storage backend.
    pub fn new(store: Arc<dyn StorageBackend>) -> Self {
        Self { store }
    }

    /// Create a pause rule for the given scope.
    pub async fn pause(&self, scope: PauseScope) -> Result<DurablePauseRule, SchedulerError> {
        let rule_id = format!("pause-rule-{}", uuid::Uuid::new_v4());
        let rule = PauseRule {
            rule_id: rule_id.clone(),
            execution_id: scope.execution_id.clone(),
            task_name: scope.task_name.clone(),
            step_pattern: scope.step_pattern.clone(),
            reason: scope.reason.clone(),
            created_at: chrono::Utc::now(),
            expires_at: scope.expires_at,
            created_by: scope.triggered_by.clone(),
            deleted_at: None,
            deleted_by: None,
        };

        let created = self
            .store
            .create_pause_rule(rule)
            .await
            .map_err(SchedulerError::Database)?;

        Ok(DurablePauseRule {
            rule_id: created.rule_id,
            scope: PauseScope {
                execution_id: created.execution_id,
                task_name: created.task_name,
                step_pattern: created.step_pattern,
                expires_at: created.expires_at,
                triggered_by: created.created_by,
                reason: created.reason.clone(),
            },
            created_at: created.created_at,
            deleted_at: created.deleted_at,
            reason: created.reason,
        })
    }

    /// Resume execution by soft-deleting matching pause rules.
    pub async fn resume(&self, scope: PauseScope) -> Result<ResumeResult, SchedulerError> {
        let filter = PauseRuleFilter {
            execution_id: scope.execution_id.clone(),
            task_name: scope.task_name.clone(),
            include_deleted: false,
        };
        let rules = self
            .store
            .list_pause_rules(filter)
            .await
            .map_err(SchedulerError::Database)?;

        let mut deleted = 0usize;
        for rule in &rules {
            if self
                .store
                .delete_pause_rule(&rule.rule_id, scope.triggered_by.as_deref())
                .await
                .map_err(SchedulerError::Database)?
            {
                deleted += 1;
            }
        }

        Ok(ResumeResult {
            rules_deleted: deleted,
        })
    }

    /// Resume by deleting a specific pause rule by ID.
    pub async fn resume_rule_by_id(
        &self,
        rule_id: &str,
        deleted_by: Option<&str>,
    ) -> Result<bool, SchedulerError> {
        self.store
            .delete_pause_rule(rule_id, deleted_by)
            .await
            .map_err(SchedulerError::Database)
    }

    /// List active pause rules.
    pub async fn list_rules(
        &self,
        filter: Option<PauseRuleFilter>,
    ) -> Result<Vec<DurablePauseRule>, SchedulerError> {
        let rules = self
            .store
            .list_pause_rules(filter.unwrap_or_default())
            .await
            .map_err(SchedulerError::Database)?;

        Ok(rules
            .into_iter()
            .map(|r| DurablePauseRule {
                rule_id: r.rule_id,
                scope: PauseScope {
                    execution_id: r.execution_id,
                    task_name: r.task_name,
                    step_pattern: r.step_pattern,
                    expires_at: r.expires_at,
                    triggered_by: r.created_by,
                    reason: r.reason.clone(),
                },
                created_at: r.created_at,
                deleted_at: r.deleted_at,
                reason: r.reason,
            })
            .collect())
    }
}
