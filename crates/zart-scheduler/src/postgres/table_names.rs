//! Table-name configuration for [`super::PostgresScheduler`].
//!
//! [`TableNames`] lets callers override the default `zart_*` table names without
//! changing any trait signatures. All seven table names are resolved **once at
//! construction time** and stored as `String`s, so every accessor is a
//! zero-allocation `&str` borrow — safe to call on hot polling paths.

/// Configures which PostgreSQL table names the scheduler will use.
///
/// The defaults (`zart_*`) match the names created by the bundled migration.
/// Override to avoid collisions or to run multiple schedulers in one database.
///
/// All resolved names are computed once when the struct is built; accessors
/// are cheap `&str` borrows with no formatting overhead.
///
/// # Example
///
/// ```rust
/// use zart_scheduler::postgres::{TableNames, TableNamesError};
///
/// // All tables prefixed with "myapp_"
/// let names = TableNames::with_prefix("myapp_").unwrap();
///
/// // Tables live in a specific schema
/// let names = TableNames::default().with_schema("tenant_a").unwrap();
///
/// // Schema + custom prefix
/// let names = TableNames::with_prefix("svc_").unwrap()
///     .with_schema("myschema").unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TableNames {
    // Kept so that `with_schema` can rebuild all names with the same prefix.
    prefix: String,
    // Pre-resolved, fully-qualified names. Computed once at construction.
    tasks: String,
    executions: String,
    execution_runs: String,
    steps: String,
    step_attempts: String,
    pause_rules: String,
    pause_snapshots: String,
}

/// Errors returned when constructing [`TableNames`] with invalid identifiers.
#[derive(Debug, thiserror::Error)]
pub enum TableNamesError {
    /// The supplied identifier is not a valid PostgreSQL identifier.
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
}

impl Default for TableNames {
    fn default() -> Self {
        Self::from_parts("zart_".to_string(), None)
    }
}

impl TableNames {
    /// Create a new `TableNames` using the default `zart_` prefix. Infallible.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `TableNames` with a custom prefix.
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if the prefix contains
    /// invalid characters, exceeds 63 characters, or does not start with a letter.
    pub fn with_prefix(prefix: impl Into<String>) -> Result<Self, TableNamesError> {
        let prefix = prefix.into();
        Self::validate_identifier(&prefix, "prefix")?;
        Ok(Self::from_parts(prefix, None))
    }

    /// Set (or replace) the schema qualifier on this `TableNames`.
    ///
    /// Consumes and returns `self` so calls can be chained:
    /// ```rust
    /// # use zart_scheduler::postgres::TableNames;
    /// let names = TableNames::with_prefix("app_").unwrap()
    ///     .with_schema("tenant").unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if the schema name is invalid.
    pub fn with_schema(self, schema: impl Into<String>) -> Result<Self, TableNamesError> {
        let schema = schema.into();
        Self::validate_identifier(&schema, "schema")?;
        Ok(Self::from_parts(self.prefix, Some(schema)))
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
    ///
    /// Reads:
    /// - `ZART_TABLE_PREFIX` — overrides the `"zart_"` default prefix.
    /// - `ZART_SCHEMA` — sets the schema qualifier (absent by default).
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if either environment
    /// variable is set but contains an invalid PostgreSQL identifier.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        let prefix = std::env::var("ZART_TABLE_PREFIX").unwrap_or_else(|_| "zart_".to_string());
        let schema = std::env::var("ZART_SCHEMA").ok();

        if let Some(ref s) = schema {
            Self::validate_identifier(s, "schema (from ZART_SCHEMA)")?;
        }
        Self::validate_identifier(&prefix, "prefix (from ZART_TABLE_PREFIX)")?;

        Ok(Self::from_parts(prefix, schema))
    }

    // ── Accessors (zero-allocation &str borrows) ────────────────────────────

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

    pub(crate) fn pause_snapshots(&self) -> &str {
        &self.pause_snapshots
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Build a fully resolved `TableNames` from raw parts.
    ///
    /// All seven table names are computed here and stored; no formatting
    /// happens again at query time.
    fn from_parts(prefix: String, schema: Option<String>) -> Self {
        let resolve = |base: &str| {
            let table = format!("{prefix}{base}");
            match &schema {
                Some(s) => format!("\"{s}\".\"{table}\""),
                None => format!("\"{table}\""),
            }
        };

        Self {
            tasks: resolve("tasks"),
            executions: resolve("executions"),
            execution_runs: resolve("execution_runs"),
            steps: resolve("steps"),
            step_attempts: resolve("step_attempts"),
            pause_rules: resolve("pause_rules"),
            pause_snapshots: resolve("pause_snapshots"),
            prefix,
        }
    }

    /// Reject identifiers that PostgreSQL would reject or that could be used
    /// for SQL injection.
    fn validate_identifier(name: &str, field: &str) -> Result<(), TableNamesError> {
        if name.is_empty() {
            return Err(TableNamesError::InvalidIdentifier(format!(
                "{field} cannot be empty"
            )));
        }
        if name.len() > 63 {
            return Err(TableNamesError::InvalidIdentifier(format!(
                "{field} exceeds PostgreSQL identifier limit (63 chars)"
            )));
        }
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(TableNamesError::InvalidIdentifier(format!(
                "{field} contains invalid characters (only alphanumeric and underscore allowed)"
            )));
        }
        if !name.chars().next().is_some_and(|c| c.is_alphabetic()) {
            return Err(TableNamesError::InvalidIdentifier(format!(
                "{field} must start with a letter"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_uses_zart_prefix_no_schema() {
        let n = TableNames::default();
        assert_eq!(n.tasks(), "\"zart_tasks\"");
        assert_eq!(n.executions(), "\"zart_executions\"");
        assert_eq!(n.execution_runs(), "\"zart_execution_runs\"");
        assert_eq!(n.steps(), "\"zart_steps\"");
        assert_eq!(n.step_attempts(), "\"zart_step_attempts\"");
        assert_eq!(n.pause_rules(), "\"zart_pause_rules\"");
        assert_eq!(n.pause_snapshots(), "\"zart_pause_snapshots\"");
    }

    #[test]
    fn with_prefix_replaces_default() {
        let n = TableNames::with_prefix("myapp_").unwrap();
        assert_eq!(n.tasks(), "\"myapp_tasks\"");
        assert_eq!(n.executions(), "\"myapp_executions\"");
        assert_eq!(n.steps(), "\"myapp_steps\"");
    }

    #[test]
    fn with_schema_qualifies_table() {
        let n = TableNames::default().with_schema("tenant_a").unwrap();
        assert_eq!(n.tasks(), "\"tenant_a\".\"zart_tasks\"");
        assert_eq!(n.steps(), "\"tenant_a\".\"zart_steps\"");
    }

    #[test]
    fn with_prefix_and_schema() {
        let n = TableNames::with_prefix("svc_")
            .unwrap()
            .with_schema("myschema")
            .unwrap();
        assert_eq!(n.tasks(), "\"myschema\".\"svc_tasks\"");
        assert_eq!(n.pause_rules(), "\"myschema\".\"svc_pause_rules\"");
    }

    #[test]
    fn validate_empty_prefix_is_rejected() {
        let err = TableNames::with_prefix("").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn validate_prefix_too_long() {
        let long = "a".repeat(64);
        let err = TableNames::with_prefix(long).unwrap_err();
        assert!(err.to_string().contains("identifier limit"));
    }

    #[test]
    fn validate_prefix_invalid_chars() {
        let err = TableNames::with_prefix("my-prefix").unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn validate_prefix_must_start_with_letter() {
        let err = TableNames::with_prefix("1prefix").unwrap_err();
        assert!(err.to_string().contains("must start with a letter"));
    }

    #[test]
    fn validate_schema_invalid_chars() {
        let err = TableNames::default().with_schema("bad-schema").unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    // Env-var tests mutate global process state and must not run concurrently.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn from_env_or_default_uses_defaults_when_vars_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: single-threaded section guarded by ENV_LOCK.
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
            std::env::remove_var("ZART_SCHEMA");
        }
        let n = TableNames::from_env_or_default().unwrap();
        assert_eq!(n.tasks(), "\"zart_tasks\"");
    }

    #[test]
    fn from_env_or_default_reads_env_vars() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: single-threaded section guarded by ENV_LOCK.
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
    }

    #[test]
    fn from_env_or_default_rejects_invalid_env_vars() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: single-threaded section guarded by ENV_LOCK.
        unsafe {
            std::env::set_var("ZART_TABLE_PREFIX", "bad-prefix");
        }
        let result = TableNames::from_env_or_default();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
        }
        assert!(result.is_err());
    }
}
