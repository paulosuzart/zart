//! Table-name configuration for [`super::PostgresTaskScheduler`].
//!
//! [`TableNames`] holds only the resolved table name(s) this crate needs.
//! All configuration — prefix, schema, validation — lives in
//! [`zart_core::table_names::TableNameConfig`]. Callers build a config first,
//! then pass it to [`TableNames::from_config`].

use zart_core::table_names::TableNameConfig;
pub use zart_core::table_names::TableNamesError;

/// Pre-resolved table names for the `zart-scheduler` task queue.
///
/// Build via [`TableNames::from_config`] after constructing a
/// [`TableNameConfig`], or use [`TableNames::default`] for the standard
/// `zart_` prefix with no schema.
///
/// # Example
///
/// ```rust
/// use zart_core::table_names::TableNameConfig;
/// use zart_scheduler::postgres::TableNames;
///
/// // Custom prefix + schema via TableNameConfig, then materialise.
/// let config = TableNameConfig::with_prefix("myapp_").unwrap()
///     .with_schema("tenant_a").unwrap();
/// let names = TableNames::from_config(config);
///
/// // Or read from environment variables.
/// let names = TableNames::from_env_or_default().unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TableNames {
    tasks: String,
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
        }
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
    ///
    /// Reads `ZART_TABLE_PREFIX` and `ZART_SCHEMA`. See
    /// [`TableNameConfig::from_env_or_default`] for details.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        Ok(Self::from_config(TableNameConfig::from_env_or_default()?))
    }

    // ── Accessor ─────────────────────────────────────────────────────────────

    pub(crate) fn tasks(&self) -> &str {
        &self.tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resolves_tasks() {
        let n = TableNames::default();
        assert_eq!(n.tasks(), "\"zart_tasks\"");
    }

    #[test]
    fn from_config_custom_prefix() {
        let config = TableNameConfig::with_prefix("sched_").unwrap();
        assert_eq!(TableNames::from_config(config).tasks(), "\"sched_tasks\"");
    }

    #[test]
    fn from_config_with_schema() {
        let config = TableNameConfig::default().with_schema("tenant_a").unwrap();
        assert_eq!(
            TableNames::from_config(config).tasks(),
            "\"tenant_a\".\"zart_tasks\""
        );
    }

    #[test]
    fn from_config_prefix_and_schema() {
        let config = TableNameConfig::with_prefix("svc_")
            .unwrap()
            .with_schema("myschema")
            .unwrap();
        assert_eq!(
            TableNames::from_config(config).tasks(),
            "\"myschema\".\"svc_tasks\""
        );
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
    }
}
