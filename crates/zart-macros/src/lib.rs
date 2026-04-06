//! Procedural macros for the Zart durable execution framework.
//!
//! These macros are **optional** — the raw `ctx.step()` API works without them.
//! They reduce boilerplate and enable a more ergonomic step-definition style.
//!
//! # Macros
//!
//! - [`#[zart_durable]`](macro@zart_durable) — annotate an async function as a durable handler,
//!   generating a unit struct that implements [`TaskHandler`](zart::registry::TaskHandler).
//! - [`z_step!`](macro@z_step) — ergonomic wrapper around `ctx.step(name, closure)`
//! - [`z_step_with_retry!`](macro@z_step_with_retry) — wrapper around `ctx.step_with_retry(name, config, closure)`
//! - [`z_wait_event!`](macro@z_wait_event) — wrapper around `ctx.wait_for_event(name, timeout)`
//! - [`z_durable_loop!`](macro@z_durable_loop) — durable `for` loop over an iterator
//!
//! # Required dependencies
//!
//! Crates using `#[zart_durable]` must also add `async-trait` to their `Cargo.toml`
//! because the generated `TaskHandler` impl requires it.
//!
//! # Example
//!
//! ```rust,ignore
//! use zart_macros::{zart_durable, z_step};
//! use zart::prelude::*;
//!
//! #[zart_durable("user-onboard", timeout = "5m")]
//! async fn onboard_user(
//!     ctx: &mut TaskContext,
//!     data: OnboardingData,
//! ) -> Result<OnboardingResult, TaskError> {
//!     z_step!("send-welcome-email", || async {
//!         Ok(send_email(&data.email).await?)
//!     }).await?;
//!
//!     Ok(OnboardingResult { /* ... */ })
//! }
//!
//! // Registers the generated struct:
//! // registry.register("user-onboard", OnboardUser);
//! ```

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    Expr, GenericArgument, Ident, ItemFn, LitStr, PathArguments, Result as SynResult, ReturnType,
    Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// ── Duration string parsing ───────────────────────────────────────────────────

/// Parse a human-readable duration string into seconds.
///
/// Accepted formats: `"5m"`, `"10s"`, `"2h"`, `"48h"`.
fn parse_duration_str(s: &str, span: Span) -> SynResult<u64> {
    if let Some(h) = s.strip_suffix('h') {
        h.parse::<u64>()
            .map(|n| n * 3600)
            .map_err(|_| syn::Error::new(span, format!("invalid hours in duration '{s}'")))
    } else if let Some(m) = s.strip_suffix('m') {
        m.parse::<u64>()
            .map(|n| n * 60)
            .map_err(|_| syn::Error::new(span, format!("invalid minutes in duration '{s}'")))
    } else if let Some(sec) = s.strip_suffix('s') {
        sec.parse::<u64>()
            .map_err(|_| syn::Error::new(span, format!("invalid seconds in duration '{s}'")))
    } else {
        Err(syn::Error::new(
            span,
            format!("duration must end with 'h', 'm', or 's' — got '{s}'"),
        ))
    }
}

// ── #[zart_durable] ──────────────────────────────────────────────────────────

/// Attribute arguments for `#[zart_durable]`.
///
/// Accepted forms:
/// - `#[zart_durable("my-task")]`
/// - `#[zart_durable("my-task", timeout = "5m")]`
struct DurableAttr {
    timeout_secs: Option<u64>,
}

impl Parse for DurableAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        // The task-name string is required (parsed but not used for code generation;
        // the struct name is derived from the function name instead).
        let _task_name: LitStr = input.parse()?;
        let mut timeout_secs = None;

        if input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            if key == "timeout" {
                timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    format!("unknown attribute key '{key}'; expected 'timeout'"),
                ));
            }
        }

        Ok(DurableAttr { timeout_secs })
    }
}

/// Annotate an async function as a Zart durable execution handler.
///
/// Generates a unit struct (named by converting the function name from
/// `snake_case` to `PascalCase`) that implements
/// [`TaskHandler`](zart::registry::TaskHandler).
///
/// The generated struct can then be registered with a [`TaskRegistry`](zart::registry::TaskRegistry):
///
/// ```rust,ignore
/// registry.register("my-task", MyTask);
/// ```
///
/// # Parameters
///
/// - First argument: the task name string (required, informational only)
/// - `timeout = "..."`: optional wall-clock timeout (`"5m"`, `"30s"`, `"2h"`)
///
/// # Function signature
///
/// The annotated function must have exactly this shape:
///
/// ```rust,ignore
/// async fn fn_name(
///     ctx: &mut TaskContext,
///     data: DataType,
/// ) -> Result<OutputType, TaskError>
/// ```
///
/// The first parameter **must** be named `ctx` when used together with
/// [`z_step!`], [`z_step_with_retry!`], or [`z_wait_event!`].
///
/// # Example
///
/// ```rust,ignore
/// #[zart_durable("send-report", timeout = "10m")]
/// async fn send_report(
///     ctx: &mut TaskContext,
///     data: ReportRequest,
/// ) -> Result<ReportId, TaskError> {
///     let id = z_step!("generate", || async { Ok(generate_report(&data).await?) }).await?;
///     Ok(id)
/// }
///
/// // Generated struct: SendReport
/// // registry.register("send-report", SendReport);
/// ```
#[proc_macro_attribute]
pub fn zart_durable(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as DurableAttr);
    let func = parse_macro_input!(item as ItemFn);

    match expand_zart_durable(args, func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_zart_durable(args: DurableAttr, func: ItemFn) -> SynResult<TokenStream2> {
    let fn_name = &func.sig.ident;
    let struct_name = snake_to_pascal(&fn_name.to_string());
    let struct_ident = format_ident!("{}", struct_name);
    let vis = &func.vis;

    // ── Validate and extract parameters ──────────────────────────────────────
    let inputs: Vec<_> = func.sig.inputs.iter().collect();
    if inputs.len() < 2 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[zart_durable] requires at least two parameters: `ctx` and `data`",
        ));
    }

    // First param (ctx): extract the pattern; type is enforced by the trait impl.
    let ctx_pat = match inputs[0] {
        syn::FnArg::Typed(pt) => &pt.pat,
        syn::FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                inputs[0],
                "#[zart_durable] cannot be applied to a method with `self`",
            ));
        }
    };

    // Second param (data): extract both pattern and type.
    let (data_pat, data_type) = match inputs[1] {
        syn::FnArg::Typed(pt) => (&pt.pat, &pt.ty),
        syn::FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                inputs[1],
                "second parameter cannot be `self`",
            ));
        }
    };

    // ── Extract the Ok-type from `Result<T, E>` ───────────────────────────────
    let ok_type = extract_ok_type(&func.sig.output)?;

    let body = &func.block;

    // ── Optional timeout method ───────────────────────────────────────────────
    let timeout_method = if let Some(secs) = args.timeout_secs {
        quote! {
            fn timeout(&self) -> ::std::option::Option<::std::time::Duration> {
                ::std::option::Option::Some(::std::time::Duration::from_secs(#secs))
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #vis struct #struct_ident;

        #[::async_trait::async_trait]
        impl ::zart::registry::TaskHandler for #struct_ident {
            type Data = #data_type;
            type Output = #ok_type;

            async fn run(
                &self,
                #ctx_pat: &mut ::zart::context::TaskContext,
                #data_pat: Self::Data,
            ) -> ::std::result::Result<Self::Output, ::zart::error::TaskError> {
                #body
            }

            #timeout_method
        }
    })
}

/// Convert a `snake_case` identifier to `PascalCase`.
///
/// Examples:
/// - `"onboard_user"` → `"OnboardUser"`
/// - `"send_report"` → `"SendReport"`
/// - `"my_task"` → `"MyTask"`
fn snake_to_pascal(s: &str) -> String {
    s.split('_')
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

/// Extract the `T` from `Result<T, E>` in a function return type.
fn extract_ok_type(ret: &ReturnType) -> SynResult<&Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Err(syn::Error::new(
                Span::call_site(),
                "#[zart_durable] function must return `Result<T, E>`",
            ));
        }
    };

    if let Type::Path(type_path) = ty.as_ref()
        && let Some(last) = type_path.path.segments.last()
        && last.ident == "Result"
        && let PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(GenericArgument::Type(ok_type)) = args.args.first()
    {
        return Ok(ok_type);
    }

    Err(syn::Error::new_spanned(
        ty,
        "return type must be `Result<T, E>`",
    ))
}

// ── z_step! ───────────────────────────────────────────────────────────────────

struct ZStepInput {
    name: Expr,
    closure: Expr,
}

impl Parse for ZStepInput {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let name = input.parse()?;
        let _: Token![,] = input.parse()?;
        let closure = input.parse()?;
        Ok(ZStepInput { name, closure })
    }
}

/// Ergonomic wrapper around [`ctx.step()`](zart::context::TaskContext::step).
///
/// Expands to `ctx.step(name, closure)`. The variable `ctx` must be in scope
/// (it is always available inside a `#[zart_durable]` handler).
///
/// # Example
///
/// ```rust,ignore
/// let email_id = z_step!("send-email", || async {
///     Ok(send_email(&data.email).await?)
/// }).await?;
/// ```
///
/// Expands to:
///
/// ```rust,ignore
/// let email_id = ctx.step("send-email", || async {
///     Ok(send_email(&data.email).await?)
/// }).await?;
/// ```
#[proc_macro]
pub fn z_step(input: TokenStream) -> TokenStream {
    let ZStepInput { name, closure } = parse_macro_input!(input as ZStepInput);
    quote! { ctx.step(#name, #closure) }.into()
}

// ── z_step_with_retry! ────────────────────────────────────────────────────────

struct ZStepRetryInput {
    name: Expr,
    config: Expr,
    closure: Expr,
}

impl Parse for ZStepRetryInput {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let name = input.parse()?;
        let _: Token![,] = input.parse()?;
        let config = input.parse()?;
        let _: Token![,] = input.parse()?;
        let closure = input.parse()?;
        Ok(ZStepRetryInput {
            name,
            config,
            closure,
        })
    }
}

/// Ergonomic wrapper around
/// [`ctx.step_with_retry()`](zart::context::TaskContext::step_with_retry).
///
/// Expands to `ctx.step_with_retry(name, config, closure)`.
///
/// # Example
///
/// ```rust,ignore
/// z_step_with_retry!(
///     "call-api",
///     RetryConfig::exponential(3, Duration::from_secs(5)),
///     || async { external_api.call().await }
/// ).await?;
/// ```
#[proc_macro]
pub fn z_step_with_retry(input: TokenStream) -> TokenStream {
    let ZStepRetryInput {
        name,
        config,
        closure,
    } = parse_macro_input!(input as ZStepRetryInput);
    quote! { ctx.step_with_retry(#name, #config, #closure) }.into()
}

// ── z_wait_event! ─────────────────────────────────────────────────────────────

struct ZWaitEventInput {
    name: Expr,
    timeout_secs: Option<u64>,
}

impl Parse for ZWaitEventInput {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let name = input.parse()?;
        let mut timeout_secs = None;

        if input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            if key == "timeout" {
                timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    format!("unknown key '{key}'; expected 'timeout'"),
                ));
            }
        }

        Ok(ZWaitEventInput { name, timeout_secs })
    }
}

/// Typed event-waiting macro.
///
/// Expands to `ctx.wait_for_event(name, timeout)`. The result type `T` is
/// inferred from the surrounding context (e.g., from the `let` binding's
/// type annotation).
///
/// # Forms
///
/// ```rust,ignore
/// // Wait indefinitely:
/// let payload: MyEvent = z_wait_event!("event-name").await?;
///
/// // Wait with a timeout:
/// let payload: MyEvent = z_wait_event!("event-name", timeout = "48h").await?;
/// ```
///
/// # Timeout format
///
/// Duration suffixes: `h` (hours), `m` (minutes), `s` (seconds).
#[proc_macro]
pub fn z_wait_event(input: TokenStream) -> TokenStream {
    let ZWaitEventInput { name, timeout_secs } = parse_macro_input!(input as ZWaitEventInput);

    let timeout_expr = match timeout_secs {
        Some(secs) => {
            quote! { ::std::option::Option::Some(::std::time::Duration::from_secs(#secs)) }
        }
        None => quote! { ::std::option::Option::None },
    };

    quote! { ctx.wait_for_event(#name, #timeout_expr) }.into()
}

// ── z_durable_loop! ───────────────────────────────────────────────────────────

struct ZDurableLoopInput {
    items: Expr,
    var: Ident,
    body: TokenStream2,
}

impl Parse for ZDurableLoopInput {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let items = input.parse()?;
        let _: Token![,] = input.parse()?;

        // Parse the closure-like `|var| { body }` syntax.
        let _: Token![|] = input.parse()?;
        let var: Ident = input.parse()?;
        let _: Token![|] = input.parse()?;

        let content;
        syn::braced!(content in input);
        let body: TokenStream2 = content.parse()?;

        Ok(ZDurableLoopInput { items, var, body })
    }
}

/// Durable loop — iterates over a collection, running the body for each item.
///
/// Expands to a plain `for` loop. When combined with [`z_step!`] inside the
/// body, each iteration's step result is cached by the framework, so re-entry
/// skips completed iterations automatically (provided each step name is unique
/// per iteration, e.g., `format!("process-{}", item.id)`).
///
/// # Example
///
/// ```rust,ignore
/// z_durable_loop!(items, |item| {
///     z_step!(&format!("process-{}", item.id), || async {
///         process_item(&item).await
///     }).await?;
/// });
/// ```
///
/// Expands to:
///
/// ```rust,ignore
/// for item in (items).into_iter() {
///     z_step!(&format!("process-{}", item.id), || async {
///         process_item(&item).await
///     }).await?;
/// }
/// ```
#[proc_macro]
pub fn z_durable_loop(input: TokenStream) -> TokenStream {
    let ZDurableLoopInput { items, var, body } = parse_macro_input!(input as ZDurableLoopInput);
    quote! {
        for #var in (#items).into_iter() {
            #body
        }
    }
    .into()
}

// ── Unit tests (compile-only) ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::snake_to_pascal;

    #[test]
    fn snake_to_pascal_basic() {
        assert_eq!(snake_to_pascal("onboard_user"), "OnboardUser");
        assert_eq!(snake_to_pascal("send_report"), "SendReport");
        assert_eq!(snake_to_pascal("my_task"), "MyTask");
        assert_eq!(snake_to_pascal("handler"), "Handler");
    }

    #[test]
    fn snake_to_pascal_with_leading_underscores() {
        // Leading underscores are stripped by the filter(non-empty)
        assert_eq!(snake_to_pascal("_private_task"), "PrivateTask");
    }

    #[test]
    fn parse_duration_hours() {
        let secs = super::parse_duration_str("2h", proc_macro2::Span::call_site()).unwrap();
        assert_eq!(secs, 7200);
    }

    #[test]
    fn parse_duration_minutes() {
        let secs = super::parse_duration_str("5m", proc_macro2::Span::call_site()).unwrap();
        assert_eq!(secs, 300);
    }

    #[test]
    fn parse_duration_seconds() {
        let secs = super::parse_duration_str("30s", proc_macro2::Span::call_site()).unwrap();
        assert_eq!(secs, 30);
    }

    #[test]
    fn parse_duration_invalid() {
        assert!(super::parse_duration_str("5x", proc_macro2::Span::call_site()).is_err());
    }
}
