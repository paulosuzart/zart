//! Procedural macros for the Zart durable execution framework.
//!
//! These macros are **optional** — the raw `ctx.execute_step()` API works without them.
//! They reduce boilerplate and enable a more ergonomic step-definition style.
//!
//! # Macros
//!
//! - [`#[zart_durable]`](macro@zart_durable) — annotate an async function as a durable handler,
//!   generating a unit struct that implements `DurableExecution`.
//! - [`#[zart_step]`](macro@zart_step) — annotate an async function as a step builder,
//!   generating a struct with an `.execute()` method.
//! - [`z_wait_event!`](macro@z_wait_event) — wrapper around `ctx.wait_for_event(name, timeout)`
//! - [`z_durable_loop!`](macro@z_durable_loop) — durable `for` loop over an iterator
//! - [`zart_capture!`](macro@zart_capture) — capture a synchronous value durably
//!
//! # Required dependencies
//!
//! Crates using `#[zart_durable]` must also add `async-trait` to their `Cargo.toml`
//! because the generated `DurableExecution` impl requires it.
//!
//! # Example
//!
//! ```rust,ignore
//! use zart_macros::zart_durable;
//! use zart::prelude::*;
//!
//! #[zart_durable("user-onboard", timeout = "5m")]
//! async fn onboard_user(
//!     ctx: &mut TaskContext,
//!     data: OnboardingData,
//! ) -> Result<OnboardingResult, TaskError> {
//!     // Use ctx.execute_step(MyStep { ... }) for step execution
//!     Ok(OnboardingResult { /* ... */ })
//! }
//!
//! // Registers the generated struct:
//! // registry.register("user-onboard", OnboardUser);
//! ```

// ── Module organization ───────────────────────────────────────────────────────

mod utils;

mod capture;
mod durable_attr;
mod durable_loop;
mod step_attr;
mod wait_event;

// ── Re-exports ────────────────────────────────────────────────────────────────

// Public macros — these are the entry points visible to users
use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn zart_durable(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as durable_attr::DurableAttr);
    let func = parse_macro_input!(item as syn::ItemFn);

    match durable_attr::expand_zart_durable(args, func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn zart_step(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as step_attr::StepAttr);
    let func = parse_macro_input!(item as syn::ItemFn);

    match step_attr::expand_zart_step(args, func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn z_wait_event(input: TokenStream) -> TokenStream {
    wait_event::expand_z_wait_event(input)
}

#[proc_macro]
pub fn z_durable_loop(input: TokenStream) -> TokenStream {
    durable_loop::expand_z_durable_loop(input)
}

#[proc_macro]
pub fn zart_capture(input: TokenStream) -> TokenStream {
    capture::expand_zart_capture(input)
}
