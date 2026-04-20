//! Table-name configuration for [`super::PostgresStorage`].
//!
//! Mirrors the `TableNames` in `zart-scheduler`; will be merged into
//! `zart-core` in Phase 3.

/// Configures which PostgreSQL table names the storage will use.
///
/// The defaults (`zart_*`) match the names created by the bundled migration.
/// Override to avoid collisions or to run multiple storages in one database.
#[derive(Debug, Clone)]
pub struct TableNames {
    prefix: String,
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
    pub fn with_prefix(prefix: impl Into<String>) -> Result<Self, TableNamesError> {
        let prefix = prefix.into();
        Self::validate_identifier(&prefix, "prefix")?;
        Ok(Self::from_parts(prefix, None))
    }

    /// Set (or replace) the schema qualifier on this `TableNames`.
    pub fn with_schema(self, schema: impl Into<String>) -> Result<Self, TableNamesError> {
        let schema = schema.into();
        Self::validate_identifier(&schema, "schema")?;
        Ok(Self::from_parts(self.prefix, Some(schema)))
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
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
