//! ZartStep trait (raw step definition without macros) and StepWithId wrapper.

use crate::retry::RetryConfig;
use crate::error::StepError;
use super::step_context::StepContext;
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
/// // Execute via TaskContext:
/// let (city, state) = ctx.execute_step(LookupZipStep { client: &client, zip_code: &data.zip_code }).await?;
/// ```
#[async_trait::async_trait]
pub trait ZartStep {
    /// The output type this step produces.
    type Output: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

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
    ///     let items = ctx.execute_step(fetch_page(page).with_id(format!("fetch-page-{page}"))).await?;
    /// }
    /// ```
    fn with_id(self, id: impl Into<String>) -> StepWithId<Self>
    where
        Self: Sized,
    {
        StepWithId {
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
    /// The `ctx` provides access to retry metadata like `current_attempt()`.
    ///
    /// **Note**: Do NOT call this directly. Use `ctx.execute_step(self)` instead,
    /// which handles retry and timeout configuration automatically.
    async fn run(&self, ctx: StepContext) -> Result<Self::Output, StepError>;
}

// ── StepWithId — call-site identity override ──────────────────────────────────

/// Wraps any [`ZartStep`] and overrides its tracking identity.
///
/// Created by [`ZartStep::with_id`]. Delegates all behaviour to the inner step
/// but reports a different name to the durable execution engine, enabling the
/// same step definition to be called multiple times (e.g. in a loop) with a
/// unique database key per call.
pub struct StepWithId<S> {
    pub(crate) inner: S,
    pub(crate) id: String,
}

#[async_trait::async_trait]
impl<S> ZartStep for StepWithId<S>
where
    S: ZartStep + Send + Sync,
{
    type Output = S::Output;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.id.clone())
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        self.inner.retry_config()
    }

    fn timeout(&self) -> Option<std::time::Duration> {
        self.inner.timeout()
    }

    async fn run(&self, ctx: StepContext) -> Result<Self::Output, StepError> {
        self.inner.run(ctx).await
    }
}
