//! PostgreSQL implementation of the [`PauseStorage`] trait.

use async_trait::async_trait;
use globset::Glob;

use super::PostgresScheduler;
use crate::StorageError;
use crate::pause_storage::{PauseRule, PauseRuleFilter, PauseSnapshot, PauseStorage};

#[async_trait]
impl PauseStorage for PostgresScheduler {
    async fn create_pause_rule(&self, rule: PauseRule) -> Result<PauseRule, StorageError> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {pause_rules}
                (rule_id, execution_id, task_name, step_pattern, created_at, expires_at, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            pause_rules = self.table_names.pause_rules(),
        ))
        .bind(&rule.rule_id)
        .bind(&rule.execution_id)
        .bind(&rule.task_name)
        .bind(&rule.step_pattern)
        .bind(rule.created_at)
        .bind(rule.expires_at)
        .bind(&rule.created_by)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rule)
    }

    async fn delete_pause_rule(
        &self,
        rule_id: &str,
        deleted_by: Option<&str>,
    ) -> Result<bool, StorageError> {
        let rows = sqlx::query(&format!(
            r#"
            UPDATE {pause_rules}
            SET deleted_at = NOW(), deleted_by = $2
            WHERE rule_id = $1 AND deleted_at IS NULL
            "#,
            pause_rules = self.table_names.pause_rules(),
        ))
        .bind(rule_id)
        .bind(deleted_by)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows > 0)
    }

    async fn list_pause_rules(
        &self,
        filter: PauseRuleFilter,
    ) -> Result<Vec<PauseRule>, StorageError> {
        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
        )> = sqlx::query_as(&format!(
            r#"
            SELECT rule_id, execution_id, task_name, step_pattern,
                   created_at, expires_at, created_by, deleted_at, deleted_by
            FROM {pause_rules}
            WHERE ($1::TEXT IS NULL OR execution_id = $1)
              AND ($2::TEXT IS NULL OR task_name = $2)
            ORDER BY created_at DESC
            "#,
            pause_rules = self.table_names.pause_rules(),
        ))
        .bind(&filter.execution_id)
        .bind(&filter.task_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .filter(|(_, _, _, _, _, _, _, deleted_at, _)| {
                filter.include_deleted || deleted_at.is_none()
            })
            .map(
                |(
                    rule_id,
                    execution_id,
                    task_name,
                    step_pattern,
                    created_at,
                    expires_at,
                    created_by,
                    deleted_at,
                    deleted_by,
                )| PauseRule {
                    rule_id,
                    execution_id,
                    task_name,
                    step_pattern,
                    created_at,
                    expires_at,
                    created_by,
                    deleted_at,
                    deleted_by,
                },
            )
            .collect())
    }

    async fn is_paused(
        &self,
        execution_id: &str,
        task_name: &str,
        step_name: Option<&str>,
    ) -> Result<bool, StorageError> {
        let rules: Vec<(Option<String>, Option<String>, Option<String>)> =
            sqlx::query_as(&format!(
                r#"
            SELECT execution_id, task_name, step_pattern
            FROM {pause_rules}
            WHERE deleted_at IS NULL
              AND (expires_at IS NULL OR expires_at > NOW())
              AND (execution_id IS NULL OR execution_id = $1)
              AND (task_name IS NULL OR task_name = $2)
            "#,
                pause_rules = self.table_names.pause_rules(),
            ))
            .bind(execution_id)
            .bind(task_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        if rules.is_empty() {
            return Ok(false);
        }

        for (rule_exec_id, rule_task_name, step_pattern) in &rules {
            let exec_matches =
                rule_exec_id.is_none() || rule_exec_id.as_deref() == Some(execution_id);
            let task_matches =
                rule_task_name.is_none() || rule_task_name.as_deref() == Some(task_name);

            if !exec_matches || !task_matches {
                continue;
            }

            let step_paused = match (step_pattern.as_deref(), step_name) {
                (None, _) => true,
                (Some(_), None) => true,
                (Some(pattern), Some(step)) => {
                    if let Ok(glob) = Glob::new(pattern) {
                        glob.compile_matcher().is_match(step)
                    } else {
                        pattern == step
                    }
                }
            };

            if step_paused {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn snapshot_pause_state(&self, snapshot: PauseSnapshot) -> Result<(), StorageError> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {pause_snapshots}
                (snapshot_id, rule_id, execution_id, run_number, completed_steps, current_data, next_step, captured_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
            pause_snapshots = self.table_names.pause_snapshots(),
        ))
        .bind(&snapshot.snapshot_id)
        .bind(&snapshot.rule_id)
        .bind(&snapshot.execution_id)
        .bind(snapshot.run_number)
        .bind(&snapshot.completed_steps)
        .bind(&snapshot.current_data)
        .bind(&snapshot.next_step)
        .bind(snapshot.captured_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }
}
