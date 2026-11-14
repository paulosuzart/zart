//! Table-name configuration for [`super::PostgresTaskScheduler`].
//!
//! [`TableNames`] holds only the resolved table name(s) this crate needs.
//! All configuration — prefix, schema, validation — lives in
//! [`zart_core::table_names::TableNameConfig`]. Callers build a config first,
//! then pass it to [`TableNames::from_config`].

use zart_core::table_names::TableNameConfig;
pub use zart_core::table_names::TableNamesError;

/// Maximum byte length of a PostgreSQL identifier (`NAMEDATALEN - 1`).
const MAX_PG_IDENT_LEN: usize = 63;

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
    wakeup_channel: String,
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
        let tasks = config.resolve("tasks");
        let wakeup_channel = derive_wakeup_channel(&tasks);
        Self {
            tasks,
            wakeup_channel,
        }
    }

    /// Build a `TableNames` from environment variables, falling back to defaults.
    ///
    /// Reads `ZART_TABLE_PREFIX` and `ZART_SCHEMA`. See
    /// [`TableNameConfig::from_env_or_default`] for details.
    pub fn from_env_or_default() -> Result<Self, TableNamesError> {
        Ok(Self::from_config(TableNameConfig::from_env_or_default()?))
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    pub(crate) fn tasks(&self) -> &str {
        &self.tasks
    }

    /// Returns the sanitized PostgreSQL `LISTEN`/`NOTIFY` wakeup channel name
    /// derived from the configured tasks table (default `"zart_tasks_wakeup"`).
    pub fn wakeup_channel(&self) -> &str {
        &self.wakeup_channel
    }
}

/// Derive a valid, tenant/prefix-isolated PostgreSQL channel name (`<= 63` bytes)
/// from the resolved `tasks` SQL identifier.
fn derive_wakeup_channel(tasks_ident: &str) -> String {
    let sanitized: String = tasks_ident
        .chars()
        .filter(|&c| c != '"')
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let full = format!("{sanitized}_wakeup");
    if full.len() <= MAX_PG_IDENT_LEN {
        return full;
    }

    // Truncate and append an 8-hex-char FNV-1a hash so distinct long prefixes/schemas
    // remain collision-resistant within PostgreSQL's 63-byte identifier limit.
    let hash = fnv1a_32(full.as_bytes());
    format!("{}_{:08x}", &full[..54], hash)
}

fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
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
        assert_eq!(n.wakeup_channel(), "myschema_env_tasks_wakeup");
    }

    #[test]
    fn wakeup_channel_default() {
        let n = TableNames::default();
        assert_eq!(n.wakeup_channel(), "zart_tasks_wakeup");
    }

    #[test]
    fn wakeup_channel_custom_prefix_and_schema_are_distinct() {
        let default_ch = TableNames::default().wakeup_channel().to_string();
        let prefix_ch = TableNames::from_config(TableNameConfig::with_prefix("sched_").unwrap())
            .wakeup_channel()
            .to_string();
        let schema_ch =
            TableNames::from_config(TableNameConfig::default().with_schema("tenant_a").unwrap())
                .wakeup_channel()
                .to_string();
        let both_ch = TableNames::from_config(
            TableNameConfig::with_prefix("svc_")
                .unwrap()
                .with_schema("myschema")
                .unwrap(),
        )
        .wakeup_channel()
        .to_string();

        assert_eq!(prefix_ch, "sched_tasks_wakeup");
        assert_eq!(schema_ch, "tenant_a_zart_tasks_wakeup");
        assert_eq!(both_ch, "myschema_svc_tasks_wakeup");

        let channels = [default_ch, prefix_ch, schema_ch, both_ch];
        for (i, ch) in channels.iter().enumerate() {
            assert!(ch.len() <= MAX_PG_IDENT_LEN);
            assert!(ch.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
            for (j, other) in channels.iter().enumerate() {
                if i != j {
                    assert_ne!(ch, other);
                }
            }
        }
    }

    #[test]
    fn wakeup_channel_truncates_over_63_bytes_without_colliding() {
        let prefix = "a".repeat(50);
        let schema_a = format!("{}1", "b".repeat(40));
        let schema_b = format!("{}2", "b".repeat(40));

        let ch_a = TableNames::from_config(
            TableNameConfig::with_prefix(&prefix)
                .unwrap()
                .with_schema(&schema_a)
                .unwrap(),
        )
        .wakeup_channel()
        .to_string();
        let ch_b = TableNames::from_config(
            TableNameConfig::with_prefix(&prefix)
                .unwrap()
                .with_schema(&schema_b)
                .unwrap(),
        )
        .wakeup_channel()
        .to_string();

        assert_eq!(ch_a.len(), MAX_PG_IDENT_LEN);
        assert_eq!(ch_b.len(), MAX_PG_IDENT_LEN);
        assert!(ch_a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        assert!(ch_b.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        assert_ne!(ch_a, ch_b);
    }
}
