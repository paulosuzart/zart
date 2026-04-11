//! ZartStep trait and NamedStep wrapper.

use crate::retry::RetryConfig;
use std::borrow::Cow;

// ── ZartStep trait (raw step definition without macros) ────────────────────────

/// A durable step definition — the trait that `#[zart_step]` implements under the hood.
///
/// Implement this trait to define a step without using the `#[zart_step]` macro.
/// The macro generates a struct and implements this trait automatically.
///
/// # Usage
///
/// ```rust,ignore
/// struct LookupZipStep<'a> { /* fields */ }
///
/// impl ZartStep for LookupZipStep<'_> { /* ... */ }
///
/// // Execute via free function:
/// let (city, state) = zart::step(LookupZipStep { client: &client, zip_code: &data.zip_code }).await?;
/// // Or simply .await (requires IntoFuture, which #[zart_step] generates):
/// let (city, state) = lookup_zip(&client, &data.zip_code).await?;
/// ```
#[async_trait::async_trait]
pub trait ZartStep {
    /// The output type this step produces.
    type Output: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

    /// The error type this step returns on failure.
    ///
    /// Must be serializable so the error survives a database round-trip for body replay.
    /// The `#[zart_step]` macro infers this from the `E` in `Result<T, E>` automatically.
    type Error: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

    /// The name of this step (used for tracking in the database).
    ///
    /// For static step names return `Cow::Borrowed("my-step")`.
    /// For dynamic names (e.g. loop iterations) return `Cow::Owned(format!("my-step-{}", n))`,
    /// or use the `{field}` template syntax in `#[zart_step]` which generates this automatically.
    fn step_name(&self) -> Cow<'static, str>;

    /// Override the step's tracking identity at the call site.
    ///
    /// Useful when the same step definition is called multiple times within a single durable
    /// handler and each call must be uniquely identified in the database.
    ///
    /// ```rust,ignore
    /// for page in 0..num_pages {
    ///     let items = zart::step(fetch_page(page).named(format!("fetch-page-{page}"))).await?;
    /// }
    /// ```
    fn named(self, id: impl Into<String>) -> NamedStep<Self>
    where
        Self: Sized,
    {
        NamedStep {
            inner: self,
            id: id.into(),
        }
    }

    /// Optional retry configuration for this step.
    ///
    /// Returns `None` for steps without retry, or `Some(config)` to enable retries.
    fn retry_config(&self) -> Option<RetryConfig> {
        None
    }

    /// Optional wall-clock timeout for this step.
    ///
    /// Returns `None` for steps without timeout, or `Some(duration)` to enable timeout.
    fn timeout(&self) -> Option<std::time::Duration> {
        None
    }

    /// Execute the step logic.
    ///
    /// Step context is accessed via `zart::context()` from within the step body.
    /// The framework scopes `Phase::Step` before calling this method.
    ///
    /// This method returns `Result<Self::Output, Self::Error>` — pure Rust, no
    /// framework types. Retry, timeout, and deadline handling are managed by the
    /// framework at the `zart::step()` boundary.
    ///
    /// **Note**: Do NOT call this directly. Use `zart::step(self)` or `.await` instead,
    /// which handles retry and timeout configuration automatically.
    async fn run(&self) -> Result<Self::Output, Self::Error>;
}

// ── NamedStep — call-site identity override ──────────────────────────────────

/// Wraps any [`ZartStep`] and overrides its tracking identity.
///
/// Created by [`ZartStep::named`]. Delegates all behaviour to the inner step
/// but reports a different name to the durable execution engine, enabling the
/// same step definition to be called multiple times (e.g. in a loop) with a
/// unique database key per call.
pub struct NamedStep<S> {
    pub(crate) inner: S,
    pub(crate) id: String,
}

#[async_trait::async_trait]
impl<S> ZartStep for NamedStep<S>
where
    S: ZartStep + Send + Sync,
{
    type Output = S::Output;
    type Error = S::Error;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.id.clone())
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        self.inner.retry_config()
    }

    fn timeout(&self) -> Option<std::time::Duration> {
        self.inner.timeout()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        self.inner.run().await
    }
}

impl<S: ZartStep + Send + Sync + 'static> std::future::IntoFuture for NamedStep<S>
where
    S::Error: std::error::Error + Send + Sync + 'static,
{
    type Output = Result<S::Output, crate::error::TaskError>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(crate::api::require(self))
    }
}
