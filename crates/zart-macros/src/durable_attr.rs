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
/// - `#[zart_durable("my-task", max_retries = 3)]`
/// - `#[zart_durable("my-task", on_failure = my_failure_handler)]`
/// - `#[zart_durable("my-task", max_retries = 3, timeout = "5m", on_failure = my_failure_handler)]`
pub struct DurableAttr {
    /// Task name string literal exposed as `TASK_NAME` on the generated struct.
    pub task_name: syn::LitStr,
    /// Optional wall-clock execution timeout in seconds.
    pub timeout_secs: Option<u64>,
    /// Optional maximum number of execution-level retries.
    pub max_retries: Option<usize>,
    /// Optional failure handler function identifier.
    pub on_failure_fn: Option<syn::Ident>,
}

impl Parse for DurableAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let task_name: syn::LitStr = input.parse()?;
        let mut timeout_secs = None;
        let mut max_retries = None;
        let mut on_failure_fn = None;

        while input.peek(syn::Token![,]) {
            let _: syn::Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![=] = input.parse()?;

            match key.to_string().as_str() {
                "timeout" => {
                    let value: syn::LitStr = input.parse()?;
                    timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
                }
                "max_retries" => {
                    let value: syn::LitInt = input.parse()?;
                    max_retries = Some(value.base10_parse::<usize>()?);
                }
                "on_failure" => {
                    on_failure_fn = Some(input.parse()?);
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown attribute key '{key}'; expected 'timeout', 'max_retries', or 'on_failure'"
                        ),
                    ));
                }
            }
        }

        Ok(DurableAttr {
            task_name,
            timeout_secs,
            max_retries,
            on_failure_fn,
        })
    }
}

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Annotate an async function as a Zart durable execution handler.
///
/// Generates a unit struct (named by converting the function name from
/// `snake_case` to `PascalCase`) that implements `DurableExecution` and
/// exposes `pub const TASK_NAME: &'static str`.
///
/// The generated struct can then be registered with a `TaskRegistry`:
///
/// ```rust,ignore
/// registry.register(MyTask::TASK_NAME, MyTask);
/// ```
///
/// # Attributes
///
/// - First argument: `"task-name"` — the task name string literal exposed as `Struct::TASK_NAME`
/// - `timeout = "..."`: optional wall-clock timeout (`"5m"`, `"30s"`, `"2h"`)
/// - `max_retries = N`: optional maximum number of execution-level retries (`usize`)
/// - `on_failure = fn_name`: optional centralized failure handler
///
/// # Function signature
///
/// The annotated function must have `data` as its parameter and return `Result<OutputType, TaskError>`:
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
/// #[zart_durable("send-report", max_retries = 3, timeout = "10m")]
/// async fn send_report(data: ReportRequest) -> Result<ReportId, TaskError> {
///     // Use zart::step(), zart::schedule(), zart::wait(), etc.
///     let id = generate_report(&data).await?;
///     Ok(id)
/// }
///
/// // Generated struct: SendReport with SendReport::TASK_NAME == "send-report"
/// // registry.register(SendReport::TASK_NAME, SendReport);
/// ```
pub(crate) fn expand_zart_durable(args: DurableAttr, func: syn::ItemFn) -> SynResult<TokenStream2> {
    let fn_name = &func.sig.ident;
    let struct_name = snake_to_pascal(&fn_name.to_string());
    let struct_ident = format_ident!("{}", struct_name);
    let vis = &func.vis;
    let task_name = &args.task_name;

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

    // ── Optional max_retries method ───────────────────────────────────────────
    let max_retries_method = if let Some(max_retries) = args.max_retries {
        quote! {
            fn max_retries(&self) -> usize {
                #max_retries
            }
        }
    } else {
        quote! {}
    };

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

        impl #struct_ident {
            /// Task name registered with `#[zart_durable]`.
            pub const TASK_NAME: &'static str = #task_name;
        }

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

            #max_retries_method

            #timeout_method

            #on_failure_method
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_task_name_and_all_attributes() {
        let attr: DurableAttr =
            syn::parse_str(r#""onboard", max_retries = 3, timeout = "5m", on_failure = handle"#)
                .expect("should parse valid durable attributes");
        assert_eq!(attr.task_name.value(), "onboard");
        assert_eq!(attr.max_retries, Some(3));
        assert_eq!(attr.timeout_secs, Some(300));
        assert_eq!(
            attr.on_failure_fn.as_ref().map(ToString::to_string),
            Some("handle".to_string())
        );
    }

    #[test]
    fn unknown_attribute_mentions_max_retries_in_error() {
        let err = syn::parse_str::<DurableAttr>(r#""onboard", bogus = 1"#)
            .err()
            .expect("unknown key should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("'timeout', 'max_retries', or 'on_failure'"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn non_integer_max_retries_is_rejected() {
        assert!(syn::parse_str::<DurableAttr>(r#""onboard", max_retries = "3""#).is_err());
        assert!(syn::parse_str::<DurableAttr>(r#""onboard", max_retries = -1"#).is_err());
    }
}
