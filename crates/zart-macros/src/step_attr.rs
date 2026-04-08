//! Implementation of the `#[zart_step]` procedural macro.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    Ident, Lifetime, LifetimeParam, Result as SynResult, ReturnType,
    parse::{Parse, ParseStream},
};

use crate::utils::{
    extract_ident_from_pattern, extract_ok_type, inject_lifetime, is_step_context_type,
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
/// - `#[zart_step("step-name", retry = "...", timeout = "...")]`
pub struct StepAttr {
    pub step_name: String,
    pub retry_config: Option<RetryAttr>,
    pub timeout_secs: Option<u64>,
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

    // Generate the run method body that calls the inner function
    let run_body = quote! {
        #inner_fn_name(#(#run_field_accesses),*, ctx).await
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

    // Extract the Output type from Result<T, StepError>
    let output_type = extract_ok_type(output)?;

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

            #step_name_method

            #retry_config_method
            #timeout_method

            async fn run(&self, ctx: ::zart::context::StepContext) -> ::std::result::Result<Self::Output, ::zart::error::StepError> {
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
/// 2. An **`.execute(&mut TaskContext)` method** that calls `ctx.execute_step()`
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
    match ctx_param {
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
                        format!(
                            "last parameter must be named `ctx` (got `{}`)",
                            pat_ident.ident
                        ),
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    &pt.pat,
                    "last parameter must be a simple identifier",
                ));
            }
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

    // Move original body to inner function with ctx as the parameter name
    let ctx_ident_for_inner = format_ident!("ctx");
    let inner_fn = if let Some(ref lifetime_param) = lifetime_param {
        quote! {
            #asyncness fn #inner_fn_name<#lifetime_param>(
                #(#struct_param_list),*,
                #ctx_ident_for_inner: ::zart::context::StepContext,
            ) #output #original_body
        }
    } else {
        quote! {
            #asyncness fn #inner_fn_name(
                #(#struct_param_list),*,
                #ctx_ident_for_inner: ::zart::context::StepContext,
            ) #output #original_body
        }
    };

    // Async executor function: build + execute in one call.
    // Usage: `validate_address_step(&mut ctx, &order_id).await?`
    let step_fn_name = format_ident!("{}_step", fn_name);
    let step_fn = if let (Some(lifetime_param), Some(_lifetime_a)) = (&lifetime_param, &lifetime_a)
    {
        quote! {
            #vis async fn #step_fn_name<#lifetime_param>(
                __ctx: &'a mut ::zart::context::TaskContext,
                #(#struct_param_list),*
            ) #output {
                __ctx.execute_step(#struct_ident { #(#field_names),* }).await
            }
        }
    } else {
        quote! {
            #vis async fn #step_fn_name(
                __ctx: &mut ::zart::context::TaskContext,
                #(#struct_param_list),*
            ) #output {
                __ctx.execute_step(#struct_ident { #(#field_names),* }).await
            }
        }
    };

    Ok(quote! {
        #struct_def

        #zart_step_impl

        #rewritten_fn

        #inner_fn

        #step_fn
    })
}
