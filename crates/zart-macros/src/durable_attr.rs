//! Implementation of the `#[zart_durable]` procedural macro.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Result as SynResult,
    parse::{Parse, ParseStream},
};

use crate::utils::{extract_ok_type, parse_duration_str, snake_to_pascal};

// ── Attribute parsing ─────────────────────────────────────────────────────────

/// Attribute arguments for `#[zart_durable]`.
///
/// Accepted forms:
/// - `#[zart_durable("my-task")]`
/// - `#[zart_durable("my-task", timeout = "5m")]`
/// - `#[zart_durable("my-task", on_failure = my_failure_handler)]`
/// - `#[zart_durable("my-task", timeout = "5m", on_failure = my_failure_handler)]`
pub struct DurableAttr {
    pub timeout_secs: Option<u64>,
    pub on_failure_fn: Option<syn::Ident>,
}

impl Parse for DurableAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        // The task-name string is required (parsed but not used for code generation;
        // the struct name is derived from the function name instead).
        let _task_name: syn::LitStr = input.parse()?;
        let mut timeout_secs = None;
        let mut on_failure_fn = None;

        while input.peek(syn::Token![,]) {
            let _: syn::Token![,] = input.parse()?;
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![=] = input.parse()?;

            match key.to_string().as_str() {
                "timeout" => {
                    let value: syn::LitStr = input.parse()?;
                    timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
                }
                "on_failure" => {
                    on_failure_fn = Some(input.parse()?);
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown attribute key '{key}'; expected 'timeout' or 'on_failure'"
                        ),
                    ));
                }
            }
        }

        Ok(DurableAttr {
            timeout_secs,
            on_failure_fn,
        })
    }
}

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Annotate an async function as a Zart durable execution handler.
///
/// Generates a unit struct (named by converting the function name from
/// `snake_case` to `PascalCase`) that implements
/// `DurableExecution`.
///
/// The generated struct can then be registered with a `TaskRegistry`:
///
/// ```rust,ignore
/// registry.register("my-task", MyTask);
/// ```
///
/// # Parameters
///
/// - First argument: `data` — the deserialized input type
/// - `timeout = "..."`: optional wall-clock timeout (`"5m"`, `"30s"`, `"2h"`)
/// - `on_failure = fn_name`: optional centralized failure handler
///
/// # Function signature
///
/// The annotated function must have exactly this shape:
///
/// ```rust,ignore
/// async fn fn_name(data: DataType) -> Result<OutputType, TaskError>
/// ```
///
/// The `on_failure` handler function signature must be:
///
/// ```rust,ignore
/// async fn handler_name(data: DataType, failure: ExecutionFailure) -> Result<OutputType, TaskError>
/// ```
///
/// # Example
///
/// ```rust,ignore
/// #[zart_durable("send-report", timeout = "10m")]
/// async fn send_report(data: ReportRequest) -> Result<ReportId, TaskError> {
///     // Use zart::step(), zart::schedule(), zart::wait(), etc.
///     let id = generate_report(&data).await?;
///     Ok(id)
/// }
///
/// // Generated struct: SendReport
/// // registry.register("send-report", SendReport);
/// ```
pub(crate) fn expand_zart_durable(args: DurableAttr, func: syn::ItemFn) -> SynResult<TokenStream2> {
    let fn_name = &func.sig.ident;
    let struct_name = snake_to_pascal(&fn_name.to_string());
    let struct_ident = format_ident!("{}", struct_name);
    let vis = &func.vis;

    // ── Validate and extract parameters ──────────────────────────────────────
    let inputs: Vec<_> = func.sig.inputs.iter().collect();
    if inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[zart_durable] requires one parameter: `data`",
        ));
    }

    // Single param (data): extract both pattern and type.
    let (data_pat, data_type) = match inputs[0] {
        syn::FnArg::Typed(pt) => (&pt.pat, &pt.ty),
        syn::FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                inputs[0],
                "#[zart_durable] cannot be applied to a method with `self`",
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

    // ── Optional on_failure method ────────────────────────────────────────────
    let on_failure_method = if let Some(ref handler_ident) = args.on_failure_fn {
        quote! {
            async fn on_failure(
                &self,
                #data_pat: Self::Data,
                failure: ::zart::error::ExecutionFailure,
            ) -> ::std::result::Result<Self::Output, ::zart::error::TaskError> {
                #handler_ident(#data_pat, failure).await
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
                #data_pat: Self::Data,
            ) -> ::std::result::Result<Self::Output, ::zart::error::TaskError> {
                #body
            }

            #timeout_method

            #on_failure_method
        }
    })
}
