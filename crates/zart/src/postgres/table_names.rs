//! Table-name configuration for [`super::PostgresStorage`].
//!
//! [`TableNames`] holds only the resolved table names this crate needs.
//! All configuration — prefix, schema, validation — lives in
//! [`zart_core::table_names::TableNameConfig`]. Callers build a config first,
//! then pass it to [`TableNames::from_config`].

use zart_core::table_names::TableNameConfig;
pub use zart_core::table_names::TableNamesError;

/// Pre-resolved table names for the `zart` storage backend.
///
/// Build via [`TableNames::from_config`] after constructing a
/// [`TableNameConfig`], or use [`TableNames::default`] for the standard
/// `zart_` prefix with no schema.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() {
/// use sqlx::PgPool;
/// use zart_core::table_names::TableNameConfig;
/// use zart::postgres::{PostgresStorage, TableNames};
///
/// let pool = PgPool::connect("postgres://...").await.unwrap();
/// let config = TableNameConfig::with_prefix("myapp_").unwrap()
///     .with_schema("tenant_a").unwrap();
/// let storage = PostgresStorage::with_table_names(pool, TableNames::from_config(config));
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TableNames {
    tasks: String,
    executions: String,
    execution_runs: String,
    steps: String,
    step_attempts: String,
    pause_rules: String,
}

impl Default for TableNames {
    fn default() -> Self {
        Self::from_config(TableNameConfig::default())
    }
}

impl TableNames {
    /// Create a `TableNames` using the default `zart_` prefix. Infallible.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct from an already-validated [`TableNameConfig`].
    ///
    /// This is the primary construction path. Configure prefix, schema, and
    /// any future options via [`TableNameConfig`], then call this.
    pub fn from_config(config: TableNameConfig) -> Self {
        Self {
            tasks: config.resolve("tasks"),
            executions: config.resolve("executions"),
            execution_runs: config.resolve("execution_runs"),
            steps: config.resolve("steps"),
            step_attempts: config.resolve("step_attempts"),
            pause_rules: config.resolve("pause_rules"),
        }
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
    ///
    /// Reads `ZART_TABLE_PREFIX` and `ZART_SCHEMA`. See
    /// [`TableNameConfig::from_env_or_default`] for details.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        Ok(Self::from_config(TableNameConfig::from_env_or_default()?))
    }

    // ── Accessors (zero-allocation &str borrows) ─────────────────────────────

    pub(crate) fn tasks(&self) -> &str {
        &self.tasks
    }

    pub(crate) fn executions(&self) -> &str {
        &self.executions
    }

    pub(crate) fn execution_runs(&self) -> &str {
        &self.execution_runs
    }

    pub(crate) fn steps(&self) -> &str {
        &self.steps
    }

    pub(crate) fn step_attempts(&self) -> &str {
        &self.step_attempts
    }

    pub(crate) fn pause_rules(&self) -> &str {
        &self.pause_rules
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resolves_all_tables() {
        let n = TableNames::default();
        assert_eq!(n.tasks(), "\"zart_tasks\"");
        assert_eq!(n.executions(), "\"zart_executions\"");
        assert_eq!(n.execution_runs(), "\"zart_execution_runs\"");
        assert_eq!(n.steps(), "\"zart_steps\"");
        assert_eq!(n.step_attempts(), "\"zart_step_attempts\"");
        assert_eq!(n.pause_rules(), "\"zart_pause_rules\"");
    }

    #[test]
    fn from_config_custom_prefix() {
        let config = TableNameConfig::with_prefix("myapp_").unwrap();
        let n = TableNames::from_config(config);
        assert_eq!(n.tasks(), "\"myapp_tasks\"");
        assert_eq!(n.executions(), "\"myapp_executions\"");
        assert_eq!(n.steps(), "\"myapp_steps\"");
        assert_eq!(n.pause_rules(), "\"myapp_pause_rules\"");
    }

    #[test]
    fn from_config_with_schema() {
        let config = TableNameConfig::default().with_schema("tenant_a").unwrap();
        let n = TableNames::from_config(config);
        assert_eq!(n.tasks(), "\"tenant_a\".\"zart_tasks\"");
        assert_eq!(n.executions(), "\"tenant_a\".\"zart_executions\"");
        assert_eq!(n.steps(), "\"tenant_a\".\"zart_steps\"");
    }

    #[test]
    fn from_config_prefix_and_schema() {
        let config = TableNameConfig::with_prefix("svc_")
            .unwrap()
            .with_schema("myschema")
            .unwrap();
        let n = TableNames::from_config(config);
        assert_eq!(n.tasks(), "\"myschema\".\"svc_tasks\"");
        assert_eq!(n.pause_rules(), "\"myschema\".\"svc_pause_rules\"");
        assert_eq!(n.step_attempts(), "\"myschema\".\"svc_step_attempts\"");
        assert_eq!(n.execution_runs(), "\"myschema\".\"svc_execution_runs\"");
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn from_env_or_default_uses_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
            std::env::remove_var("ZART_SCHEMA");
        }
        let n = TableNames::from_env_or_default().unwrap();
        assert_eq!(n.tasks(), "\"zart_tasks\"");
        assert_eq!(n.executions(), "\"zart_executions\"");
    }

    #[test]
    fn from_env_or_default_reads_env_vars() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("ZART_TABLE_PREFIX", "env_");
            std::env::set_var("ZART_SCHEMA", "myschema");
        }
        let n = TableNames::from_env_or_default().unwrap();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
            std::env::remove_var("ZART_SCHEMA");
        }
        assert_eq!(n.tasks(), "\"myschema\".\"env_tasks\"");
        assert_eq!(n.steps(), "\"myschema\".\"env_steps\"");
    }
}
