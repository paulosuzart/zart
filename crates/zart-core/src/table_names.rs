//! PostgreSQL table-name resolution logic shared across Zart crates.
//!
//! [`TableNameConfig`] holds the prefix and optional schema, validates them,
//! and resolves any base table name into a fully-qualified SQL identifier.
//! Each crate defines its own `TableNames` struct that stores the resolved
//! strings and calls [`TableNameConfig::resolve`] once at construction time.
//!
//! # Separation of concerns
//!
//! | Concern | Location |
//! |---|---|
//! | Validation, prefix/schema formatting | `zart-core::table_names` (here) |
//! | Which tables exist | each consuming crate |
//! | Pre-resolved accessor strings | each consuming crate |
//!
//! This keeps `zart-scheduler` self-contained: if it is ever extracted into
//! a standalone project it only needs to vendor or re-implement this small
//! config type, not the full execution-side table set.

/// Errors returned when constructing a [`TableNameConfig`] with invalid identifiers.
#[derive(Debug, thiserror::Error)]
pub enum TableNamesError {
    /// The supplied prefix or schema is not a valid PostgreSQL identifier.
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
}

/// Shared prefix/schema configuration used to resolve PostgreSQL table names.
///
/// Create one via [`TableNameConfig::with_prefix`], [`TableNameConfig::default`],
/// or [`TableNameConfig::from_env_or_default`], then call [`TableNameConfig::resolve`]
/// once per table during the construction of a crate-local `TableNames` struct.
///
/// # Example
///
/// ```rust
/// use zart_core::table_names::TableNameConfig;
///
/// let config = TableNameConfig::with_prefix("myapp_").unwrap()
///     .with_schema("tenant_a").unwrap();
///
/// assert_eq!(config.resolve("tasks"), "\"tenant_a\".\"myapp_tasks\"");
/// assert_eq!(config.resolve("executions"), "\"tenant_a\".\"myapp_executions\"");
/// ```
#[derive(Debug, Clone)]
pub struct TableNameConfig {
    prefix: String,
    schema: Option<String>,
}

impl Default for TableNameConfig {
    /// Returns a config with `"zart_"` prefix and no schema qualifier.
    fn default() -> Self {
        Self {
            prefix: "zart_".to_string(),
            schema: None,
        }
    }
}

impl TableNameConfig {
    /// Create a config with a custom prefix.
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if the prefix contains
    /// invalid characters, exceeds 63 characters, or does not start with a letter.
    pub fn with_prefix(prefix: impl Into<String>) -> Result<Self, TableNamesError> {
        let prefix = prefix.into();
        Self::validate_identifier(&prefix, "prefix")?;
        Ok(Self {
            prefix,
            schema: None,
        })
    }

    /// Set (or replace) the schema qualifier, consuming `self`.
    ///
    /// Storing the config alongside the resolved strings means this can be
    /// forwarded from a crate-local `TableNames::with_schema` without
    /// re-implementing validation.
    ///
    /// ```rust
    /// # use zart_core::table_names::TableNameConfig;
    /// let config = TableNameConfig::with_prefix("app_").unwrap()
    ///     .with_schema("tenant").unwrap();
    /// assert_eq!(config.resolve("tasks"), "\"tenant\".\"app_tasks\"");
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if the schema name is invalid.
    pub fn with_schema(self, schema: impl Into<String>) -> Result<Self, TableNamesError> {
        let schema = schema.into();
        Self::validate_identifier(&schema, "schema")?;
        Ok(Self {
            schema: Some(schema),
            ..self
        })
    }

    /// Build a config from environment variables, falling back to defaults.
    ///
    /// Reads:
    /// - `ZART_TABLE_PREFIX` — overrides the `"zart_"` default prefix.
    /// - `ZART_SCHEMA` — sets the schema qualifier (absent by default).
    ///
    /// # Errors
    ///
    /// Returns [`TableNamesError::InvalidIdentifier`] if either variable is
    /// set but contains an invalid PostgreSQL identifier.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        let prefix = std::env::var("ZART_TABLE_PREFIX").unwrap_or_else(|_| "zart_".to_string());
        let schema = std::env::var("ZART_SCHEMA").ok();

        if let Some(ref s) = schema {
            Self::validate_identifier(s, "schema (from ZART_SCHEMA)")?;
        }
        Self::validate_identifier(&prefix, "prefix (from ZART_TABLE_PREFIX)")?;

        Ok(Self { prefix, schema })
    }

    /// Resolve a base table name into a fully-qualified, double-quoted SQL identifier.
    ///
    /// Call this once per table during `TableNames` construction — the result
    /// should be stored in a `String` field so query-time access is allocation-free.
    ///
    /// ```rust
    /// # use zart_core::table_names::TableNameConfig;
    /// let config = TableNameConfig::default();
    /// assert_eq!(config.resolve("tasks"), "\"zart_tasks\"");
    ///
    /// let config = TableNameConfig::default().with_schema("myschema").unwrap();
    /// assert_eq!(config.resolve("steps"), "\"myschema\".\"zart_steps\"");
    /// ```
    pub fn resolve(&self, base: &str) -> String {
        let table = format!("{}{}", self.prefix, base);
        match &self.schema {
            Some(s) => format!("\"{s}\".\"{table}\""),
            None => format!("\"{table}\""),
        }
    }

    // ── Validation ──────────────────────────────────────────────────────────

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

    // ── TableNameConfig::resolve ─────────────────────────────────────────────

    #[test]
    fn resolve_default_prefix_no_schema() {
        let c = TableNameConfig::default();
        assert_eq!(c.resolve("tasks"), "\"zart_tasks\"");
        assert_eq!(c.resolve("executions"), "\"zart_executions\"");
        assert_eq!(c.resolve("steps"), "\"zart_steps\"");
    }

    #[test]
    fn resolve_custom_prefix() {
        let c = TableNameConfig::with_prefix("myapp_").unwrap();
        assert_eq!(c.resolve("tasks"), "\"myapp_tasks\"");
        assert_eq!(c.resolve("steps"), "\"myapp_steps\"");
    }

    #[test]
    fn resolve_with_schema() {
        let c = TableNameConfig::default().with_schema("tenant_a").unwrap();
        assert_eq!(c.resolve("tasks"), "\"tenant_a\".\"zart_tasks\"");
        assert_eq!(c.resolve("steps"), "\"tenant_a\".\"zart_steps\"");
    }

    #[test]
    fn resolve_prefix_and_schema() {
        let c = TableNameConfig::with_prefix("svc_")
            .unwrap()
            .with_schema("myschema")
            .unwrap();
        assert_eq!(c.resolve("tasks"), "\"myschema\".\"svc_tasks\"");
        assert_eq!(c.resolve("pause_rules"), "\"myschema\".\"svc_pause_rules\"");
    }

    #[test]
    fn with_schema_replaces_existing_schema() {
        let c = TableNameConfig::default()
            .with_schema("first")
            .unwrap()
            .with_schema("second")
            .unwrap();
        assert_eq!(c.resolve("tasks"), "\"second\".\"zart_tasks\"");
    }

    // ── Validation ───────────────────────────────────────────────────────────

    #[test]
    fn validate_empty_prefix_rejected() {
        let err = TableNameConfig::with_prefix("").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn validate_prefix_too_long() {
        let long = "a".repeat(64);
        let err = TableNameConfig::with_prefix(long).unwrap_err();
        assert!(err.to_string().contains("identifier limit"));
    }

    #[test]
    fn validate_prefix_invalid_chars() {
        let err = TableNameConfig::with_prefix("my-prefix").unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn validate_prefix_must_start_with_letter() {
        let err = TableNameConfig::with_prefix("1prefix").unwrap_err();
        assert!(err.to_string().contains("must start with a letter"));
    }

    #[test]
    fn validate_schema_invalid_chars() {
        let err = TableNameConfig::default()
            .with_schema("bad-schema")
            .unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    // ── from_env_or_default ──────────────────────────────────────────────────

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn from_env_uses_defaults_when_vars_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
            std::env::remove_var("ZART_SCHEMA");
        }
        let c = TableNameConfig::from_env_or_default().unwrap();
        assert_eq!(c.resolve("tasks"), "\"zart_tasks\"");
    }

    #[test]
    fn from_env_reads_prefix_and_schema() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("ZART_TABLE_PREFIX", "env_");
            std::env::set_var("ZART_SCHEMA", "myschema");
        }
        let c = TableNameConfig::from_env_or_default().unwrap();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
            std::env::remove_var("ZART_SCHEMA");
        }
        assert_eq!(c.resolve("tasks"), "\"myschema\".\"env_tasks\"");
    }

    #[test]
    fn from_env_rejects_invalid_prefix() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("ZART_TABLE_PREFIX", "bad-prefix");
        }
        let result = TableNameConfig::from_env_or_default();
        unsafe {
            std::env::remove_var("ZART_TABLE_PREFIX");
        }
        assert!(result.is_err());
    }
}
