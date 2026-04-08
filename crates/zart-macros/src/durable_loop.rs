//! Implementation of the `z_durable_loop!` function-like macro.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Expr, Ident, Result as SynResult, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// ── Input parsing ─────────────────────────────────────────────────────────────

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

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Durable loop — iterates over a collection, running the body for each item.
///
/// Expands to a plain `for` loop. When combined with `ctx.execute_step()` inside the
/// body, each iteration's step result is cached by the framework, so re-entry
/// skips completed iterations automatically (provided each step name is unique
/// per iteration, e.g., `format!("process-{}", item.id)`).
///
/// # Example
///
/// ```rust,ignore
/// for item in items {
///     let step = ProcessItemStep { item: item.clone() };
///     ctx.execute_step(step).await?;
/// }
/// ```
///
/// Expands to:
///
/// ```rust,ignore
/// for item in (items).into_iter() {
///     // body
/// }
/// ```
pub(crate) fn expand_z_durable_loop(input: TokenStream) -> TokenStream {
    let ZDurableLoopInput { items, var, body } = parse_macro_input!(input as ZDurableLoopInput);
    quote! {
        for #var in (#items).into_iter() {
            #body
        }
    }
    .into()
}
