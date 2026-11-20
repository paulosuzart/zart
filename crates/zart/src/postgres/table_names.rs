//! Table-name configuration for [`super::PostgresStorage`].
//!
//! [`TableNames`] stores the underlying [`TableNameConfig`] alongside the
//! pre-resolved SQL identifiers this crate needs. Validation and identifier
//! formatting live in [`zart_core::table_names::TableNameConfig`]; callers can
//! build [`TableNames`] directly via [`TableNames::with_prefix`] /
//! [`TableNames::with_schema`], [`TableNames::from_config`], or
//! [`TableNames::from_env_or_default`].

use zart_core::table_names::TableNameConfig;
pub use zart_core::table_names::TableNamesError;

/// Pre-resolved table names for the `zart` storage backend.
///
/// Build via [`TableNames::with_prefix`], [`TableNames::with_schema`],
/// [`TableNames::from_config`], [`TableNames::from_env_or_default`], or
/// [`TableNames::default`] for the standard `zart_` prefix with no schema.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() {
/// use sqlx::PgPool;
/// use zart::postgres::{PostgresStorage, TableNames};
///
/// let pool = PgPool::connect("postgres://...").await.unwrap();
/// let names = TableNames::with_prefix("myapp_").unwrap()
///     .with_schema("tenant_a").unwrap();
/// let storage = PostgresStorage::with_table_names(pool, names);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TableNames {
    config: TableNameConfig,
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

    /// Create a `TableNames` with a custom table-name prefix and no schema qualifier.
    ///
    /// Delegates validation to [`TableNameConfig::with_prefix`].
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if `prefix` is invalid.
    pub fn with_prefix(prefix: impl Into<String>) -> Result<Self, TableNamesError> {
        Ok(Self::from_config(TableNameConfig::with_prefix(prefix)?))
    }

    /// Set (or replace) the schema qualifier, consuming `self` and re-resolving all table names.
    ///
    /// Delegates validation to [`TableNameConfig::with_schema`].
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if `schema` is invalid.
    pub fn with_schema(self, schema: impl Into<String>) -> Result<Self, TableNamesError> {
        Ok(Self::from_config(self.config.with_schema(schema)?))
    }

    /// Construct from an already-validated [`TableNameConfig`].
    ///
    /// Use this when sharing a [`TableNameConfig`] across crates, or construct
    /// directly via [`TableNames::with_prefix`], [`TableNames::with_schema`],
    /// or [`TableNames::from_env_or_default`].
    pub fn from_config(config: TableNameConfig) -> Self {
        Self {
            tasks: config.resolve("tasks"),
            executions: config.resolve("executions"),
            execution_runs: config.resolve("execution_runs"),
            steps: config.resolve("steps"),
            step_attempts: config.resolve("step_attempts"),
            pause_rules: config.resolve("pause_rules"),
            config,
        }
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
    ///
    /// Reads `ZART_TABLE_PREFIX` and `ZART_SCHEMA`. See
    /// [`TableNameConfig::from_env_or_default`] for details.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        Ok(Self::from_config(TableNameConfig::from_env_or_default()?))
    }

    /// Returns the underlying [`TableNameConfig`] used to resolve these names.
    pub fn config(&self) -> &TableNameConfig {
        &self.config
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

    #[test]
    fn with_prefix_and_with_schema_build_and_expose_config() {
        let names = TableNames::with_prefix("svc_")
            .unwrap()
            .with_schema("tenant_a")
            .unwrap();
        assert_eq!(names.tasks(), "\"tenant_a\".\"svc_tasks\"");
        assert_eq!(names.executions(), "\"tenant_a\".\"svc_executions\"");
        assert_eq!(
            names.execution_runs(),
            "\"tenant_a\".\"svc_execution_runs\""
        );
        assert_eq!(names.steps(), "\"tenant_a\".\"svc_steps\"");
        assert_eq!(names.step_attempts(), "\"tenant_a\".\"svc_step_attempts\"");
        assert_eq!(names.pause_rules(), "\"tenant_a\".\"svc_pause_rules\"");
        assert_eq!(names.config().resolve("tasks"), names.tasks());
    }

    #[test]
    fn with_schema_on_default_and_invalid_inputs_rejected() {
        let names = TableNames::new().with_schema("analytics").unwrap();
        assert_eq!(names.tasks(), "\"analytics\".\"zart_tasks\"");

        let replaced = names.with_schema("billing").unwrap();
        assert_eq!(replaced.tasks(), "\"billing\".\"zart_tasks\"");

        assert!(TableNames::with_prefix("bad-prefix").is_err());
        assert!(TableNames::new().with_schema("bad-schema").is_err());
    }
}
