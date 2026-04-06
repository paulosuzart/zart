//! Procedural macros for the Zart durable execution framework.
//!
//! These macros are **optional** — the raw `ctx.step()` API works without them.
//! They reduce boilerplate and enable a more ergonomic step-definition style.
//!
//! # Macros
//!
//! - [`#[zart_durable]`](macro@zart_durable) — annotate an async function as a durable handler,
//!   generating a unit struct that implements [`DurableExecution`](zart::registry::DurableExecution).
//! - [`#[zart_step]`](macro@zart_step) — annotate an async function as a step builder,
//!   generating a struct with an `.execute()` method.
//! - [`z_step!`](macro@z_step) — ergonomic wrapper around `ctx.step(name, closure)`
//! - [`z_step_with_retry!`](macro@z_step_with_retry) — wrapper around `ctx.step_with_retry(name, config, closure)`
//! - [`z_wait_event!`](macro@z_wait_event) — wrapper around `ctx.wait_for_event(name, timeout)`
//! - [`z_durable_loop!`](macro@z_durable_loop) — durable `for` loop over an iterator
//!
//! # Required dependencies
//!
//! Crates using `#[zart_durable]` must also add `async-trait` to their `Cargo.toml`
//! because the generated `DurableExecution` impl requires it.
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
    Lifetime, LifetimeParam,
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
/// [`DurableExecution`](zart::registry::DurableExecution).
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
        impl ::zart::registry::DurableExecution for #struct_ident {
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
/// The closure receives a [`StepContext`](zart::context::StepContext) so you can access
/// execution metadata like `sctx.current_attempt()`, `sctx.max_retries()`, and
/// `sctx.is_retry_attempt()`.
///
/// # Example
///
/// ```rust,ignore
/// let email_id = z_step!("send-email", |sctx| async move {
///     println!("Attempt: {}", sctx.current_attempt());
///     Ok(send_email(&data.email).await?)
/// }).await?;
/// ```
///
/// Expands to:
///
/// ```rust,ignore
/// let email_id = ctx.step("send-email", |sctx| async move {
///     println!("Attempt: {}", sctx.current_attempt());
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
/// The closure receives a [`StepContext`](zart::context::StepContext) so you can access
/// execution metadata like `sctx.current_attempt()`, `sctx.max_retries()`, and
/// `sctx.is_retry_attempt()`.
///
/// # Example
///
/// ```rust,ignore
/// z_step_with_retry!(
///     "call-api",
///     RetryConfig::exponential(3, Duration::from_secs(5)),
///     |sctx| async move {
///         if sctx.current_attempt() == 0 {
///             // Simulate transient failure on first attempt
///             return Err(StepError::Failed { step: "call-api".into(), reason: "timeout".into() });
///         }
///         external_api.call().await
///     }
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

// ── #[zart_step] ──────────────────────────────────────────────────────────────

/// Attribute arguments for `#[zart_step]`.
///
/// Accepted forms:
/// - `#[zart_step("step-name")]`
/// - `#[zart_step("step-name", retry = "fixed(3, 1s)")]`
/// - `#[zart_step("step-name", retry = "exponential(3, 1s)")]`
/// - `#[zart_step("step-name", timeout = "5m")]`
/// - `#[zart_step("step-name", retry = "...", timeout = "...")]`
struct StepAttr {
    step_name: String,
    retry_config: Option<RetryAttr>,
    timeout_secs: Option<u64>,
}

/// Parsed retry attribute.
enum RetryAttr {
    Fixed { attempts: usize, delay_ms: u64 },
    Exponential { attempts: usize, delay_ms: u64 },
}

impl Parse for StepAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let step_name: LitStr = input.parse()?;
        let mut retry_config = None;
        let mut timeout_secs = None;

        while input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;

            match key.to_string().as_str() {
                "retry" => {
                    retry_config = Some(parse_retry_attr(&value.value(), value.span())?);
                }
                "timeout" => {
                    timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown attribute key '{key}'; expected 'retry' or 'timeout'"),
                    ));
                }
            }
        }

        Ok(StepAttr {
            step_name: step_name.value(),
            retry_config,
            timeout_secs,
        })
    }
}

/// Parse retry attribute string like "fixed(3, 1s)" or "exponential(5, 2s)".
fn parse_retry_attr(s: &str, span: Span) -> SynResult<RetryAttr> {
    if let Some(inner) = s.strip_prefix("fixed(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return Err(syn::Error::new(
                span,
                format!("fixed retry must be 'fixed(n, duration)' — got '{s}'"),
            ));
        }
        let attempts: usize = parts[0].parse().map_err(|_| {
            syn::Error::new(span, format!("invalid attempt count '{}'", parts[0]))
        })?;
        let delay_ms = parse_duration_to_ms(parts[1], span)?;
        Ok(RetryAttr::Fixed { attempts, delay_ms })
    } else if let Some(inner) = s
        .strip_prefix("exponential(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return Err(syn::Error::new(
                span,
                format!("exponential retry must be 'exponential(n, duration)' — got '{s}'"),
            ));
        }
        let attempts: usize = parts[0].parse().map_err(|_| {
            syn::Error::new(span, format!("invalid attempt count '{}'", parts[0]))
        })?;
        let delay_ms = parse_duration_to_ms(parts[1], span)?;
        Ok(RetryAttr::Exponential { attempts, delay_ms })
    } else {
        Err(syn::Error::new(
            span,
            format!("retry must be 'fixed(n, duration)' or 'exponential(n, duration)' — got '{s}'"),
        ))
    }
}

/// Parse a duration string to milliseconds.
fn parse_duration_to_ms(s: &str, span: Span) -> SynResult<u64> {
    let secs = parse_duration_str(s, span)?;
    Ok(secs * 1000)
}

/// Annotate an async function as a Zart step function.
///
/// Transforms a plain async function into a **step builder** that can be executed
/// via `.execute(&mut TaskContext)`.
///
/// # Function signature
///
/// ```rust,ignore
/// #[zart_step("step-name", retry = "exponential(3, 1s)")]
/// async fn my_step(
///     // ... any number of parameters (become struct fields)
///     ctx: StepContext,   // ← must be the LAST parameter
/// ) -> Result<T, StepError>
/// ```
///
/// # Generated code
///
/// The macro generates:
/// 1. A **struct** capturing all parameters except `StepContext`
/// 2. An **`.execute(&mut TaskContext)` method** that calls `ctx.step()` or `ctx.step_with_retry()`
/// 3. Rewrites the original function to return the struct (builder pattern)
/// 4. Moves the original body to a private `_inner` function
///
/// # Example
///
/// ```rust,ignore
/// #[zart_step("lookup-zip", retry = "exponential(3, 1s)")]
/// async fn lookup_zip(
///     client: &reqwest::Client,
///     zip_code: &str,
///     ctx: StepContext,
/// ) -> Result<(String, String), StepError> {
///     // ... step logic
/// }
///
/// // Usage in durable handler:
/// let (city, state) = lookup_zip(&client, &data.zip_code).execute(ctx).await?;
/// ```
///
/// # Attributes
///
/// | Attribute | Required | Description |
/// |---|---|---|
/// | `"step-name"` | Yes | The name used for step tracking in the database. |
/// | `retry = "..."` | No | Retry configuration. Supports `fixed(n, duration)` and `exponential(n, duration)`. |
/// | `timeout = "..."` | No | Step timeout. Supports duration strings like `"5m"`, `"30s"`, `"2h"`. |
#[proc_macro_attribute]
pub fn zart_step(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as StepAttr);
    let func = parse_macro_input!(item as ItemFn);

    match expand_zart_step(args, func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_zart_step(args: StepAttr, func: ItemFn) -> SynResult<TokenStream2> {
    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let asyncness = &func.sig.asyncness;
    let output = &func.sig.output;
    let original_body = &func.block;

    // Validate asyncness
    if asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[zart_step] can only be applied to async functions",
        ));
    }

    // Validate return type is Result<_, StepError>
    validate_step_return_type(output)?;

    // Extract parameters
    let inputs: Vec<_> = func.sig.inputs.iter().collect();
    if inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[zart_step] requires at least one parameter: `ctx: StepContext`",
        ));
    }

    // Last parameter must be ctx: StepContext
    let ctx_param = inputs.last().unwrap();
    let (ctx_pat, ctx_type) = match ctx_param {
        syn::FnArg::Typed(pt) => {
            // Check type is StepContext (allowing for path like StepContext or zart::context::StepContext)
            if !is_step_context_type(&pt.ty) {
                return Err(syn::Error::new_spanned(
                    &pt.ty,
                    "last parameter must be `ctx: StepContext`",
                ));
            }
            // Check that the parameter is named `ctx`
            if let syn::Pat::Ident(pat_ident) = &*pt.pat {
                if pat_ident.ident != "ctx" {
                    return Err(syn::Error::new_spanned(
                        &pt.pat,
                        format!("last parameter must be named `ctx` (got `{}`)", pat_ident.ident),
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    &pt.pat,
                    "last parameter must be a simple identifier",
                ));
            }
            (&pt.pat, &pt.ty)
        }
        syn::FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                ctx_param,
                "last parameter cannot be `self`",
            ));
        }
    };

    // All parameters except the last become struct fields
    let struct_params: Vec<_> = inputs.iter().take(inputs.len() - 1).cloned().collect();

    // Generate struct name: snake_case -> PascalCase + "Step"
    let struct_name = format!("{}Step", snake_to_pascal(&fn_name.to_string()));
    let struct_ident = format_ident!("{}", struct_name);
    let inner_fn_name = format_ident!("{}_inner", fn_name);
    
    // Define lifetime 'a that will be used throughout
    let lifetime_a = Lifetime::new("'a", Span::call_site());
    let lifetime_param = LifetimeParam::new(lifetime_a.clone());

    // Generate struct fields with lifetime injected into references
    let struct_fields: Vec<_> = struct_params
        .iter()
        .filter_map(|param| match param {
            syn::FnArg::Typed(pt) => {
                let pat = &pt.pat;
                let ty_with_lifetime = inject_lifetime(&pt.ty, &lifetime_a);
                Some(quote! { #pat: #ty_with_lifetime })
            }
            _ => None,
        })
        .collect();

    // Generate field names for struct initialization
    let field_names: Vec<_> = struct_params
        .iter()
        .filter_map(|param| match param {
            syn::FnArg::Typed(pt) => extract_ident_from_pattern(&pt.pat),
            _ => None,
        })
        .collect();
    
    // Generate parameter list for function signatures (with lifetime injected)
    let struct_param_list: Vec<_> = struct_params
        .iter()
        .filter_map(|param| match param {
            syn::FnArg::Typed(pt) => {
                let pat = &pt.pat;
                let ty_with_lifetime = inject_lifetime(&pt.ty, &lifetime_a);
                Some(quote! { #pat: #ty_with_lifetime })
            }
            _ => None,
        })
        .collect();

    // Generate the execute method
    let execute_method = generate_execute_method(
        &args,
        &struct_ident,
        &lifetime_a,
        &field_names,
        ctx_pat,
        ctx_type,
        output,
        &inner_fn_name,
    )?;

    // Generate the struct with lifetime parameter 'a
    let struct_def = quote! {
        #vis struct #struct_ident<#lifetime_param> {
            #(#struct_fields),*
        }
    };

    // Rewrite original function to return the builder struct (not async)
    let rewritten_fn = quote! {
        #vis fn #fn_name<#lifetime_param>(
            #(#struct_param_list),*
        ) -> #struct_ident<#lifetime_a> {
            #struct_ident {
                #(#field_names),*
            }
        }
    };

    // Move original body to inner function with ctx as the parameter name
    let ctx_ident_for_inner = format_ident!("ctx");
    let inner_fn = quote! {
        #asyncness fn #inner_fn_name<#lifetime_param>(
            #(#struct_param_list),*,
            #ctx_ident_for_inner: ::zart::context::StepContext,
        ) #output #original_body
    };

    Ok(quote! {
        #struct_def

        impl<#lifetime_param> #struct_ident<#lifetime_a> {
            #execute_method
        }

        #rewritten_fn

        #inner_fn
    })
}

/// Generate the `.execute()` method for the step struct.
fn generate_execute_method(
    args: &StepAttr,
    _struct_ident: &Ident,
    _lifetime_a: &Lifetime,
    field_names: &[Ident],
    ctx_pat: &Box<syn::Pat>,
    _ctx_type: &Box<syn::Type>,
    output: &ReturnType,
    inner_fn_name: &Ident,
) -> SynResult<TokenStream2> {
    let step_name = &args.step_name;

    // Generate field access expressions (self.field_name)
    let field_accesses: Vec<_> = field_names.iter().map(|ident| quote! { self.#ident }).collect();

    // Generate the closure body that calls the inner function
    let closure_body = quote! {
        #inner_fn_name(#(#field_accesses),*, ctx).await
    };

    // Build the step execution call based on attributes
    let step_call = if args.retry_config.is_some() {
        // With retry
        let retry_config_expr = generate_retry_config_expr(&args.retry_config)?;
        quote! {
            ctx.step_with_retry(
                #step_name,
                #retry_config_expr,
                |ctx| async move {
                    #closure_body
                }
            ).await
        }
    } else if args.timeout_secs.is_some() {
        // With timeout only
        let timeout_secs = args.timeout_secs.unwrap();
        quote! {
            ctx.step_with_timeout(
                #step_name,
                ::std::time::Duration::from_secs(#timeout_secs),
                |ctx| async move {
                    #closure_body
                }
            ).await
        }
    } else {
        // No retry, no timeout
        quote! {
            ctx.step(
                #step_name,
                |ctx| async move {
                    #closure_body
                }
            ).await
        }
    };

    Ok(quote! {
        pub async fn execute(
            self,
            #ctx_pat: &'a mut ::zart::context::TaskContext,
        ) #output {
            #step_call
        }
    })
}

/// Generate retry config expression from parsed attributes.
fn generate_retry_config_expr(retry_attr: &Option<RetryAttr>) -> SynResult<TokenStream2> {
    match retry_attr {
        Some(RetryAttr::Fixed { attempts, delay_ms }) => Ok(quote! {
            ::zart::retry::RetryConfig::fixed(#attempts, ::std::time::Duration::from_millis(#delay_ms))
        }),
        Some(RetryAttr::Exponential { attempts, delay_ms }) => Ok(quote! {
            ::zart::retry::RetryConfig::exponential(#attempts, ::std::time::Duration::from_millis(#delay_ms))
        }),
        None => Err(syn::Error::new(
            Span::call_site(),
            "retry config required for this code path",
        )),
    }
}

/// Inject lifetime 'a into reference types
fn inject_lifetime(ty: &Type, lifetime_a: &Lifetime) -> TokenStream2 {
    match ty {
        Type::Reference(type_ref) => {
            let elem = inject_lifetime(&type_ref.elem, lifetime_a);
            if let Some(mut_token) = &type_ref.mutability {
                if type_ref.lifetime.is_some() {
                    // Already has a lifetime, keep it
                    quote! { &#mut_token #elem }
                } else {
                    // Inject our lifetime
                    quote! { &#lifetime_a #mut_token #elem }
                }
            } else {
                if type_ref.lifetime.is_some() {
                    quote! { &#elem }
                } else {
                    quote! { &#lifetime_a #elem }
                }
            }
        }
        _ => quote! { #ty },
    }
}

/// Extract identifier from a pattern (handles simple ident patterns like `client`, `zip_code`, etc.)
fn extract_ident_from_pattern(pat: &syn::Pat) -> Option<Ident> {
    match pat {
        syn::Pat::Ident(pat_ident) => Some(pat_ident.ident.clone()),
        _ => None, // We only support simple identifier patterns for now
    }
}

/// Check if a type is `StepContext` (allowing for various paths).
fn is_step_context_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(last) = type_path.path.segments.last() {
            return last.ident == "StepContext";
        }
    }
    false
}

/// Validate that the return type is `Result<_, StepError>`.
fn validate_step_return_type(output: &ReturnType) -> SynResult<()> {
    let ty = match output {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new(
                Span::call_site(),
                "#[zart_step] function must return `Result<T, StepError>`",
            ));
        }
    };

    if let Type::Path(type_path) = ty {
        if let Some(last) = type_path.path.segments.last() {
            if last.ident == "Result" {
                // Good enough — we'll trust the user on the error type
                return Ok(());
            }
        }
    }

    Err(syn::Error::new_spanned(
        ty,
        "return type must be `Result<T, StepError>`",
    ))
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

    #[test]
    fn step_struct_name_generation() {
        assert_eq!(
            format!("{}Step", snake_to_pascal("lookup_zip")),
            "LookupZipStep"
        );
        assert_eq!(
            format!("{}Step", snake_to_pascal("find_breweries")),
            "FindBreweriesStep"
        );
    }
}
