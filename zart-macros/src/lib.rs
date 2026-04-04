//! Procedural macros for the Zart durable execution framework.
//!
//! These macros are **optional** — the raw `ctx.step()` API works without them.
//! They reduce boilerplate and enable automatic step tracking at compile time.
//!
//! # Macros (implemented in M6)
//!
//! - [`#[zart_durable]`](macro@zart_durable) — annotate a function as a durable handler
//! - [`z_step!`](macro@z_step) — ergonomic wrapper around `ctx.step()`
//! - [`z_step_with_retry!`](macro@z_step_with_retry) — step with retry config
//! - [`z_wait_event!`](macro@z_wait_event) — typed event waiting
//! - [`z_durable_loop!`](macro@z_durable_loop) — loop-aware step tracking
//!
//! # Example (M6)
//!
//! ```rust,ignore
//! use zart_macros::zart_durable;
//!
//! #[zart_durable("user-onboard", timeout = "5m")]
//! async fn onboard_user(
//!     ctx: &mut TaskContext<impl Scheduler>,
//!     data: OnboardingData,
//! ) -> Result<OnboardingResult, TaskError> {
//!     z_step!("send-welcome-email", || async {
//!         Ok(send_email(&data.email).await?)
//!     }).await?;
//!
//!     Ok(OnboardingResult { /* ... */ })
//! }
//! ```

use proc_macro::TokenStream;

/// Annotates an async function as a Zart durable execution handler.
///
/// Generates a `TaskHandler` implementation and optionally configures a timeout.
///
/// **Implemented in M6.**
///
/// # Example
///
/// ```rust,ignore
/// #[zart_durable("my-task", timeout = "10m")]
/// async fn my_handler(ctx: &mut TaskContext<impl Scheduler>, data: MyData)
///     -> Result<MyOutput, TaskError>
/// {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn zart_durable(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // TODO(M6): parse attribute arguments (task name, timeout) and generate TaskHandler impl.
    // For now, pass through unchanged so annotated code compiles.
    item
}

/// Ergonomic wrapper around `ctx.step()` with automatic step-name registration.
///
/// **Implemented in M6.**
///
/// # Example
///
/// ```rust,ignore
/// let email_id = z_step!("send-email", || async {
///     Ok(send_email(&data.email).await?)
/// }).await?;
/// ```
#[proc_macro]
pub fn z_step(input: TokenStream) -> TokenStream {
    // TODO(M6): expand to ctx.step(name, lambda) with step registration side-effects.
    input
}

/// Ergonomic wrapper around `ctx.step_with_retry()`.
///
/// **Implemented in M6.**
///
/// # Example
///
/// ```rust,ignore
/// z_step_with_retry!(
///     "call-api",
///     RetryConfig::exponential(3, Duration::from_secs(5)),
///     || async { external.call().await }
/// ).await?;
/// ```
#[proc_macro]
pub fn z_step_with_retry(input: TokenStream) -> TokenStream {
    // TODO(M6): expand to ctx.step_with_retry(name, config, lambda).
    input
}

/// Typed event-waiting macro.
///
/// **Implemented in M6.**
///
/// # Example
///
/// ```rust,ignore
/// let approval: ApprovalData = z_wait_event!("manager-approval", timeout = "48h").await?;
/// ```
#[proc_macro]
pub fn z_wait_event(input: TokenStream) -> TokenStream {
    // TODO(M6): expand to ctx.wait_for_event::<T>(name, timeout).
    input
}

/// Loop wrapper that maintains iteration state for durable re-entry.
///
/// **Implemented in M6.**
///
/// # Example
///
/// ```rust,ignore
/// z_durable_loop!(items, |item| {
///     z_step!(format!("process-{}", item.id), || async {
///         process_item(item).await
///     }).await?;
/// });
/// ```
#[proc_macro]
pub fn z_durable_loop(input: TokenStream) -> TokenStream {
    // TODO(M6): expand to a loop that tracks iteration state in execution context.
    input
}
