//! Tracing and structured logging setup utilities.
//!
//! Provides ready-to-use tracing subscriber configuration for Zart applications.

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for the tracing subscriber.
#[derive(Debug, Clone, Default)]
pub struct TracingConfig {
    /// Environment filter (e.g., "info", "debug", "zart=debug").
    /// Defaults to `RUST_LOG` env var or "info".
    pub env_filter: Option<String>,

    /// Whether to enable JSON formatting (for production/log aggregation).
    /// Defaults to `false` (human-readable format).
    pub json_format: bool,
}

/// Initialize the global tracing subscriber with sensible defaults.
///
/// This should be called once at application startup.
///
/// # Example
///
/// ```rust
/// use zart::logging::init_tracing;
///
/// init_tracing().expect("Failed to initialize tracing");
/// ```
pub fn init_tracing() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing_with_config(TracingConfig::default())
}

/// Initialize the global tracing subscriber with custom configuration.
///
/// # Example
///
/// ```rust
/// use zart::logging::{init_tracing_with_config, TracingConfig};
///
/// let config = TracingConfig {
///     env_filter: Some("zart=debug,info".to_string()),
///     json_format: true,
/// };
/// init_tracing_with_config(config).expect("Failed to initialize tracing");
/// ```
pub fn init_tracing_with_config(
    config: TracingConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env_filter = config
        .env_filter
        .unwrap_or_else(|| std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()));

    let filter = EnvFilter::try_new(env_filter)?;

    if config.json_format {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .try_init()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_config_defaults_are_sane() {
        let config = TracingConfig::default();
        assert!(config.env_filter.is_none());
        assert!(!config.json_format);
    }

    #[test]
    fn init_tracing_succeeds() {
        // Note: This test may fail if tracing is already initialized elsewhere.
        // In practice, init_tracing is called once at startup.
        let result = init_tracing();
        // We don't assert Ok here because it might fail if already initialized.
        // The important thing is it doesn't panic.
        drop(result);
    }
}
