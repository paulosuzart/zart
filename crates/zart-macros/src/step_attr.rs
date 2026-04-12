//! Implementation of the `#[zart_step]` procedural macro.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    Ident, Lifetime, LifetimeParam, Result as SynResult, ReturnType,
    parse::{Parse, ParseStream},
};

use crate::utils::{
    extract_error_type, extract_ident_from_pattern, extract_ok_type, inject_lifetime,
    parse_duration_str, parse_duration_to_ms, parse_step_name_template, snake_to_pascal,
    type_has_references, validate_step_return_type,
};

// ── Attribute parsing ─────────────────────────────────────────────────────────

/// Attribute arguments for `#[zart_step]`.
///
/// Accepted forms:
/// - `#[zart_step("step-name")]`
/// - `#[zart_step("step-name", retry = "fixed(3, 1s)")]`
/// - `#[zart_step("step-name", retry = "exponential(3, 1s)")]`
/// - `#[zart_step("step-name", timeout = "5m")]`
/// - `#[zart_step("step-name", timeout = "5m", timeout_scope = "global")]`
/// - `#[zart_step("step-name", timeout = "30s", timeout_scope = "per_attempt")]`
/// - `#[zart_step("step-name", retry = "...", timeout = "...")]`
pub struct StepAttr {
    pub step_name: String,
    pub retry_config: Option<RetryAttr>,
    pub timeout_secs: Option<u64>,
    pub timeout_scope: Option<String>,
}

/// Parsed retry attribute.
pub enum RetryAttr {
    Fixed { attempts: usize, delay_ms: u64 },
    Exponential { attempts: usize, delay_ms: u64 },
}

impl Parse for StepAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let step_name: syn::LitStr = input.parse()?;
        let mut retry_config = None;
        let mut timeout_secs = None;
        let mut timeout_scope = None;

        while input.peek(syn::Token![,]) {
            let _: syn::Token![,] = input.parse()?;
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![=] = input.parse()?;
            let value: syn::LitStr = input.parse()?;

            match key.to_string().as_str() {
                "retry" => {
                    retry_config = Some(parse_retry_attr(&value.value(), value.span())?);
                }
                "timeout" => {
                    timeout_secs = Some(parse_duration_str(&value.value(), value.span())?);
                }
                "timeout_scope" => {
                    timeout_scope = Some(value.value());
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown attribute key '{key}'; expected 'retry', 'timeout', or 'timeout_scope'"
                        ),
                    ));
                }
            }
        }

        Ok(StepAttr {
            step_name: step_name.value(),
            retry_config,
            timeout_secs,
            timeout_scope,
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
        let attempts: usize = parts[0]
            .parse()
            .map_err(|_| syn::Error::new(span, format!("invalid attempt count '{}'", parts[0])))?;
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
        let attempts: usize = parts[0]
            .parse()
            .map_err(|_| syn::Error::new(span, format!("invalid attempt count '{}'", parts[0])))?;
        let delay_ms = parse_duration_to_ms(parts[1], span)?;
        Ok(RetryAttr::Exponential { attempts, delay_ms })
    } else {
        Err(syn::Error::new(
            span,
            format!("retry must be 'fixed(n, duration)' or 'exponential(n, duration)' — got '{s}'"),
        ))
    }
}

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Generate the `impl ZartStep` trait implementation for the step struct.
fn generate_zart_step_impl(
    args: &StepAttr,
    struct_ident: &Ident,
    lifetime_a: Option<&Lifetime>,
    struct_params: &[&syn::FnArg],
    _field_names: &[Ident],
    output: &ReturnType,
    inner_fn_name: &Ident,
) -> SynResult<TokenStream2> {
    let step_name = &args.step_name;

    // Build the step_name() method — static or dynamic based on {field} templates.
    let step_name_method = match parse_step_name_template(step_name) {
        None => {
            // Plain static name — zero-cost Cow::Borrowed.
            quote! {
                fn step_name(&self) -> ::std::borrow::Cow<'static, str> {
                    ::std::borrow::Cow::Borrowed(#step_name)
                }
            }
        }
        Some((fmt_str, fields)) => {
            // Template name — generate Cow::Owned(format!(...)) using struct fields.
            let field_idents: Vec<_> = fields.iter().map(|f| format_ident!("{}", f)).collect();
            quote! {
                fn step_name(&self) -> ::std::borrow::Cow<'static, str> {
                    ::std::borrow::Cow::Owned(::std::format!(#fmt_str, #(self.#field_idents),*))
                }
            }
        }
    };

    // Generate field access expressions for run method (clone owned types, reference refs)
    let run_field_accesses: Vec<_> = struct_params
        .iter()
        .filter_map(|param| match param {
            syn::FnArg::Typed(pt) => {
                let ident = extract_ident_from_pattern(&pt.pat)?;
                // Check if the type is a reference
                if type_has_references(&pt.ty) {
                    Some(quote! { self.#ident })
                } else {
                    // Owned type - needs clone
                    Some(quote! { self.#ident.clone() })
                }
            }
            _ => None,
        })
        .collect();

    // Generate the run method body that calls the inner function (no ctx arg)
    let run_body = quote! {
        #inner_fn_name(#(#run_field_accesses),*).await
    };

    // Generate retry_config method
    let retry_config_method = if let Some(retry_attr) = &args.retry_config {
        let retry_expr = generate_retry_config_expr(retry_attr)?;
        quote! {
            fn retry_config(&self) -> ::std::option::Option<::zart::retry::RetryConfig> {
                ::std::option::Option::Some(#retry_expr)
            }
        }
    } else {
        quote! {} // Uses trait default (None)
    };

    // Generate timeout method
    let timeout_method = if let Some(timeout_secs) = args.timeout_secs {
        quote! {
            fn timeout(&self) -> ::std::option::Option<::std::time::Duration> {
                ::std::option::Option::Some(::std::time::Duration::from_secs(#timeout_secs))
            }
        }
    } else {
        quote! {} // Uses trait default (None)
    };

    // Generate timeout_scope method
    let timeout_scope_method = if let Some(scope_str) = &args.timeout_scope {
        let scope_ident = match scope_str.as_str() {
            "global" => quote! { ::zart::timeout::TimeoutScope::Global },
            "per_attempt" => quote! { ::zart::timeout::TimeoutScope::PerAttempt },
            _ => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "invalid timeout_scope '{scope_str}'; expected 'global' or 'per_attempt'"
                    ),
                ));
            }
        };
        quote! {
            fn timeout_scope(&self) -> ::zart::timeout::TimeoutScope {
                #scope_ident
            }
        }
    } else {
        quote! {} // Uses trait default (Global)
    };

    // Extract the Output type and Error type from Result<T, E>
    let output_type = extract_ok_type(output)?;
    let error_type = extract_error_type(output)?;

    // Generate impl header with or without lifetime
    let impl_header = if let Some(lifetime_a) = lifetime_a {
        quote! {
            impl<#lifetime_a> ::zart::context::ZartStep for #struct_ident<#lifetime_a>
        }
    } else {
        quote! {
            impl ::zart::context::ZartStep for #struct_ident
        }
    };

    Ok(quote! {
        #[::async_trait::async_trait]
        #impl_header {
            type Output = #output_type;
            type Error = #error_type;

            #step_name_method

            #retry_config_method
            #timeout_method
            #timeout_scope_method

            async fn run(&self) -> ::std::result::Result<Self::Output, Self::Error> {
                #run_body
            }
        }
    })
}

/// Generate retry config expression from parsed attributes.
fn generate_retry_config_expr(retry_attr: &RetryAttr) -> SynResult<TokenStream2> {
    match retry_attr {
        RetryAttr::Fixed { attempts, delay_ms } => Ok(quote! {
            ::zart::retry::RetryConfig::fixed(#attempts, ::std::time::Duration::from_millis(#delay_ms))
        }),
        RetryAttr::Exponential { attempts, delay_ms } => Ok(quote! {
            ::zart::retry::RetryConfig::exponential(#attempts, ::std::time::Duration::from_millis(#delay_ms))
        }),
    }
}

/// Annotate an async function as a Zart step function.
///
/// Transforms a plain async function into a **step builder** that can be `.await`ed
/// directly. Step context is accessed via `zart::context()` inside the function body.
///
/// # Function signature
///
/// ```rust,ignore
/// #[zart_step("step-name", retry = "exponential(3, 1s)")]
/// async fn my_step(
///     // ... any number of parameters (become struct fields)
/// ) -> Result<T, StepError>
/// ```
///
/// # Generated code
///
/// The macro generates:
/// 1. A **struct** capturing all parameters
/// 2. A `ZartStep` impl with `run()` (no ctx parameter)
/// 3. An `IntoFuture` impl so `step_name(args).await` works
/// 4. Rewrites the original function to return the struct (builder pattern)
/// 5. Moves the original body to a private `_inner` function
///
/// # Example
///
/// ```rust,ignore
/// #[zart_step("lookup-zip", retry = "exponential(3, 1s)")]
/// async fn lookup_zip(
///     client: &reqwest::Client,
///     zip_code: &str,
/// ) -> Result<(String, String), StepError> {
///     if zart::context().is_retry() { /* ... */ }
///     // ... step logic
/// }
///
/// // Usage in durable handler:
/// let (city, state) = lookup_zip(&client, &data.zip_code).await?;
/// ```
///
/// # Attributes
///
/// | Attribute | Required | Description |
/// |---|---|---|
/// | `"step-name"` | Yes | The name used for step tracking in the database. Supports `{field}` template for dynamic names. |
/// | `retry = "..."` | No | Retry configuration. Supports `fixed(n, duration)` and `exponential(n, duration)`. |
/// | `timeout = "..."` | No | Step timeout. Supports duration strings like `"5m"`, `"30s"`, `"2h"`, `"3d"`. |
/// | `timeout_scope = "..."` | No | Timeout scope: `"global"` (default, deadline shared across retries) or `"per_attempt"` (fresh countdown each attempt). |
pub(crate) fn expand_zart_step(args: StepAttr, func: syn::ItemFn) -> SynResult<TokenStream2> {
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

    // Extract parameters — all parameters become struct fields (no ctx required).
    let inputs: Vec<_> = func.sig.inputs.iter().collect();

    // All parameters become struct fields
    let struct_params: Vec<_> = inputs.to_vec();

    // Generate struct name: snake_case -> PascalCase + "Step"
    let struct_name = format!("{}Step", snake_to_pascal(&fn_name.to_string()));
    let struct_ident = format_ident!("{}", struct_name);
    let inner_fn_name = format_ident!("{}_inner", fn_name);

    // Check if any parameters contain references - only then do we need a lifetime
    let has_references = struct_params.iter().any(|param| {
        if let syn::FnArg::Typed(pt) = param {
            type_has_references(&pt.ty)
        } else {
            false
        }
    });

    // Generate lifetime and struct components
    let (lifetime_a, lifetime_param, struct_fields, field_names, struct_param_list) =
        if has_references {
            let lifetime_a = Lifetime::new("'a", Span::call_site());
            let lifetime_param = LifetimeParam::new(lifetime_a.clone());

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

            let field_names: Vec<_> = struct_params
                .iter()
                .filter_map(|param| match param {
                    syn::FnArg::Typed(pt) => extract_ident_from_pattern(&pt.pat),
                    _ => None,
                })
                .collect();

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

            (
                Some(lifetime_a),
                Some(lifetime_param),
                struct_fields,
                field_names,
                struct_param_list,
            )
        } else {
            let struct_fields: Vec<_> = struct_params
                .iter()
                .filter_map(|param| match param {
                    syn::FnArg::Typed(pt) => {
                        let pat = &pt.pat;
                        let ty = &pt.ty;
                        Some(quote! { #pat: #ty })
                    }
                    _ => None,
                })
                .collect();

            let field_names: Vec<_> = struct_params
                .iter()
                .filter_map(|param| match param {
                    syn::FnArg::Typed(pt) => extract_ident_from_pattern(&pt.pat),
                    _ => None,
                })
                .collect();

            let struct_param_list: Vec<_> = struct_params
                .iter()
                .filter_map(|param| match param {
                    syn::FnArg::Typed(pt) => {
                        let pat = &pt.pat;
                        let ty = &pt.ty;
                        Some(quote! { #pat: #ty })
                    }
                    _ => None,
                })
                .collect();

            (None, None, struct_fields, field_names, struct_param_list)
        };

    // Generate the ZartStep trait implementation
    let zart_step_impl = generate_zart_step_impl(
        &args,
        &struct_ident,
        lifetime_a.as_ref(),
        &struct_params,
        &field_names,
        output,
        &inner_fn_name,
    )?;

    // Generate the struct
    let struct_def = if let Some(ref lifetime_param) = lifetime_param {
        quote! {
            #vis struct #struct_ident<#lifetime_param> {
                #(#struct_fields),*
            }
        }
    } else {
        quote! {
            #vis struct #struct_ident {
                #(#struct_fields),*
            }
        }
    };

    // Rewrite original function to return the builder struct (not async)
    let rewritten_fn =
        if let (Some(lifetime_param), Some(lifetime_a)) = (&lifetime_param, &lifetime_a) {
            quote! {
                #vis fn #fn_name<#lifetime_param>(
                    #(#struct_param_list),*
                ) -> #struct_ident<#lifetime_a> {
                    #struct_ident {
                        #(#field_names),*
                    }
                }
            }
        } else {
            quote! {
                #vis fn #fn_name(
                    #(#struct_param_list),*
                ) -> #struct_ident {
                    #struct_ident {
                        #(#field_names),*
                    }
                }
            }
        };

    // Move original body to inner function (no ctx param)
    let inner_fn = if let Some(ref lifetime_param) = lifetime_param {
        quote! {
            #asyncness fn #inner_fn_name<#lifetime_param>(
                #(#struct_param_list),*
            ) #output #original_body
        }
    } else {
        quote! {
            #asyncness fn #inner_fn_name(
                #(#struct_param_list),*
            ) #output #original_body
        }
    };

    // IntoFuture — makes `step_name(args).await` work
    // Delegates to zart::require() for fail-fast semantics.
    let output_type = extract_ok_type(output)?;
    let into_future_impl = if let Some(ref lifetime_a) = lifetime_a {
        quote! {
            impl<#lifetime_a> ::std::future::IntoFuture for #struct_ident<#lifetime_a>
            where
                <Self as ::zart::context::ZartStep>::Error: ::std::error::Error + Send + Sync + 'static,
            {
                type Output = ::std::result::Result<#output_type, ::zart::error::TaskError>;
                type IntoFuture = ::std::pin::Pin<Box<dyn ::std::future::Future<Output = Self::Output> + Send + #lifetime_a>>;

                fn into_future(self) -> Self::IntoFuture {
                    Box::pin(::zart::require(self))
                }
            }
        }
    } else {
        quote! {
            impl ::std::future::IntoFuture for #struct_ident
            where
                <Self as ::zart::context::ZartStep>::Error: ::std::error::Error + Send + Sync + 'static,
            {
                type Output = ::std::result::Result<#output_type, ::zart::error::TaskError>;
                type IntoFuture = ::std::pin::Pin<Box<dyn ::std::future::Future<Output = Self::Output> + Send>>;

                fn into_future(self) -> Self::IntoFuture {
                    Box::pin(::zart::require(self))
                }
            }
        }
    };

    Ok(quote! {
        #struct_def

        #zart_step_impl

        #into_future_impl

        #rewritten_fn

        #inner_fn
    })
}
