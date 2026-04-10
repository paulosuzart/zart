//! Implementation of the `z_wait_event!` function-like macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, LitStr, Result as SynResult, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

use crate::utils::parse_duration_str;

// ── Input parsing ─────────────────────────────────────────────────────────────

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
            let key: syn::Ident = input.parse()?;
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

// ── Macro expansion ───────────────────────────────────────────────────────────

/// Typed event-waiting macro.
///
/// Expands to `zart::wait_for_event(name, timeout)`. The result type `T` is
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
pub(crate) fn expand_z_wait_event(input: TokenStream) -> TokenStream {
    let ZWaitEventInput { name, timeout_secs } = parse_macro_input!(input as ZWaitEventInput);

    let timeout_expr = match timeout_secs {
        Some(secs) => {
            quote! { ::std::option::Option::Some(::std::time::Duration::from_secs(#secs)) }
        }
        None => quote! { ::std::option::Option::None },
    };

    quote! { ::zart::wait_for_event(#name, #timeout_expr) }.into()
}
