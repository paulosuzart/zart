//! ZartStep trait and NamedStep wrapper.

use crate::retry::RetryConfig;
use crate::timeout::TimeoutScope;
use std::borrow::Cow;

// ── ZartStep trait (raw step definition without macros) ────────────────────────

/// A single durable step inside a [`DurableExecution`] workflow.
///
/// The framework executes `run()` once and persists the result. On every
/// subsequent re-entry of the parent handler, `run()` is **not called again**
/// — the stored result is deserialized and returned immediately. This
/// makes steps the unit of idempotency in Zart.
///
/// # Defining a step
///
/// The preferred way is the `#[zart_step]` proc-macro, which generates the
/// struct, impl, and an `IntoFuture` so the step can be `.await`-ed directly:
///
/// ```rust,ignore
/// #[zart_step("charge-payment")]
/// async fn charge_payment(
///     &self,
///     client: &PaymentClient,
///     order_id: OrderId,
/// ) -> Result<ChargeReceipt, PaymentError> {
///     client.charge(order_id).await
/// }
///
/// // Inside a DurableExecution::run():
/// let receipt = charge_payment(&client, order_id).await?;
/// ```
///
/// Implement the trait directly when you need finer control — for example,
/// when step input carries references or when you want the step struct to own
/// a connection pool:
///
/// ```rust,ignore
/// struct ChargePayment {
///     client: PaymentClient, // owned
///     order_id: OrderId,
/// }
///
/// #[async_trait::async_trait]
/// impl ZartStep for ChargePayment {
///     type Output = ChargeReceipt;
///     type Error  = PaymentError;
///
///     fn step_name(&self) -> std::borrow::Cow<'static, str> {
///         "charge-payment".into()
///     }
///
///     async fn run(&self) -> Result<ChargeReceipt, PaymentError> {
///         self.client.charge(self.order_id).await
///     }
/// }
///
/// // Inside DurableExecution::run():
/// let receipt = zart::require(ChargePayment { client: client.clone(), order_id }).await?;
/// ```
///
/// # Step identity and loops
///
/// Each step is identified in the database by the string returned from
/// [`step_name`](ZartStep::step_name). When the same step type is called
/// multiple times in a loop, each call must have a unique name — otherwise
/// the second call will return the cached result of the first:
///
/// ```rust,ignore
/// for (i, chunk) in chunks.iter().enumerate() {
///     zart::require(ProcessChunk { chunk }.named(format!("process-chunk-{i}"))).await?;
/// }
/// ```
///
/// # Retries and timeouts
///
/// Override [`retry_config`](ZartStep::retry_config) to enable per-step retry
/// with exponential back-off, and [`timeout`](ZartStep::timeout) to cap the
/// wall-clock time per attempt. The framework handles scheduling retries
/// automatically; `run()` always sees a clean call with no prior state.
///
/// [`DurableExecution`]: crate::registry::DurableExecution
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

    /// The scope of this step's timeout.
    ///
    /// - `TimeoutScope::Global` (default): the timeout is a deadline calculated from the
    ///   first attempt. All retries must complete within this window.
    /// - `TimeoutScope::PerAttempt`: each retry attempt gets a fresh countdown.
    fn timeout_scope(&self) -> TimeoutScope {
        TimeoutScope::Global
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

    fn timeout_scope(&self) -> TimeoutScope {
        self.inner.timeout_scope()
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
