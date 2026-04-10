//! Implementation of the `zart_capture!` function-like macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, LitStr, Result as SynResult, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// ── Input parsing ─────────────────────────────────────────────────────────────

struct ZCaptureInput {
    name: LitStr,
    expr: Expr,
}

impl Parse for ZCaptureInput {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let name: LitStr = input.parse()?;
        if name.value().is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "capture step name must not be empty",
            ));
        }
        let _: Token![,] = input.parse()?;
        let expr: Expr = input.parse()?;
        Ok(ZCaptureInput { name, expr })
    }
}

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Capture a synchronous, pure value durably.
///
/// Expands to `zart::capture("name", || expr).await?`.
///
/// On first body run: evaluates the expression, writes the result as a completed step row,
/// returns the value — body walk continues without parking.
/// On replay: returns the cached DB value; the expression is never evaluated.
///
/// The first argument must be a string literal (the stable step ID).
/// The second argument is an expression — the macro wraps it in a closure automatically.
///
/// # Example
///
/// ```rust,ignore
/// let started_at = zart_capture!("started-at", chrono::Utc::now());
/// let user_tz    = zart_capture!("user-tz", env::var("TZ").unwrap_or_default());
/// ```
pub(crate) fn expand_zart_capture(input: TokenStream) -> TokenStream {
    let ZCaptureInput { name, expr } = parse_macro_input!(input as ZCaptureInput);
    quote! { ::zart::capture(#name, || #expr).await? }.into()
}
