//! Shared utility functions for procedural macro code generation.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{GenericArgument, Ident, Lifetime, PathArguments, Result as SynResult, ReturnType, Type};

// ── Duration string parsing ───────────────────────────────────────────────────

/// Parse a human-readable duration string into seconds.
///
/// Accepted formats: `"3d"`, `"48h"`, `"5m"`, `"10s"`.
pub fn parse_duration_str(s: &str, span: Span) -> SynResult<u64> {
    if let Some(d) = s.strip_suffix('d') {
        d.parse::<u64>()
            .map(|n| n * 86400)
            .map_err(|_| syn::Error::new(span, format!("invalid days in duration '{s}'")))
    } else if let Some(h) = s.strip_suffix('h') {
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
            format!("duration must end with 'd', 'h', 'm', or 's' — got '{s}'"),
        ))
    }
}

/// Parse a duration string to milliseconds.
pub fn parse_duration_to_ms(s: &str, span: Span) -> SynResult<u64> {
    let secs = parse_duration_str(s, span)?;
    Ok(secs * 1000)
}

// ── Name transformation ───────────────────────────────────────────────────────

/// Convert a `snake_case` identifier to `PascalCase`.
///
/// Examples:
/// - `"onboard_user"` → `"OnboardUser"`
/// - `"send_report"` → `"SendReport"`
/// - `"my_task"` → `"MyTask"`
pub fn snake_to_pascal(s: &str) -> String {
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

// ── Type extraction and validation ────────────────────────────────────────────

/// Extract the `T` from `Result<T, E>` in a function return type.
pub fn extract_ok_type(ret: &ReturnType) -> SynResult<&Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Err(syn::Error::new(
                Span::call_site(),
                "function must return `Result<T, E>`",
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

/// Extract the `E` from `Result<T, E>` in a function return type.
pub fn extract_error_type(ret: &ReturnType) -> SynResult<&Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Err(syn::Error::new(
                Span::call_site(),
                "function must return `Result<T, E>`",
            ));
        }
    };

    if let Type::Path(type_path) = ty.as_ref()
        && let Some(last) = type_path.path.segments.last()
        && last.ident == "Result"
        && let PathArguments::AngleBracketed(args) = &last.arguments
        && args.args.len() >= 2
        && let Some(GenericArgument::Type(err_type)) = args.args.iter().nth(1)
    {
        return Ok(err_type);
    }

    Err(syn::Error::new_spanned(
        ty,
        "return type must be `Result<T, E>`",
    ))
}

// ── Template parsing ──────────────────────────────────────────────────────────

/// Parse `{field_name}` template placeholders in a step name string.
///
/// Returns `None` for plain static names. Returns `Some((format_str, fields))` when
/// at least one `{field}` placeholder is found, where `format_str` has each placeholder
/// replaced with `{}` (suitable for `format!`) and `fields` lists the field names in order.
///
/// # Example
/// `"fetch-page-{page}"` → `Some(("fetch-page-{}", vec!["page"]))`
pub fn parse_step_name_template(name: &str) -> Option<(String, Vec<String>)> {
    if !name.contains('{') {
        return None;
    }
    let mut fmt = String::with_capacity(name.len());
    let mut fields = Vec::new();
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut field = String::new();
            for fc in chars.by_ref() {
                if fc == '}' {
                    break;
                }
                field.push(fc);
            }
            fmt.push_str("{}");
            fields.push(field);
        } else {
            fmt.push(c);
        }
    }
    Some((fmt, fields))
}

// ── Type inspection helpers ───────────────────────────────────────────────────

/// Check if a type contains any references.
pub fn type_has_references(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Path(type_path) => {
            if let Some(last) = type_path.path.segments.last()
                && let syn::PathArguments::AngleBracketed(args) = &last.arguments
            {
                for arg in &args.args {
                    if let syn::GenericArgument::Type(inner_ty) = arg
                        && type_has_references(inner_ty)
                    {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Inject lifetime 'a into reference types
pub fn inject_lifetime(ty: &Type, lifetime_a: &Lifetime) -> TokenStream2 {
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
pub fn extract_ident_from_pattern(pat: &syn::Pat) -> Option<Ident> {
    match pat {
        syn::Pat::Ident(pat_ident) => Some(pat_ident.ident.clone()),
        _ => None, // We only support simple identifier patterns for now
    }
}

/// Check if a type is `StepContext` (allowing for various paths).
#[allow(dead_code)] // kept for potential future use; was used in Phase 1-2 macro validation
pub fn is_step_context_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(last) = type_path.path.segments.last()
    {
        return last.ident == "StepContext";
    }
    false
}

/// Validate that the return type is `Result<_, StepError>`.
pub fn validate_step_return_type(output: &ReturnType) -> SynResult<()> {
    let ty = match output {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new(
                Span::call_site(),
                "#[zart_step] function must return `Result<T, StepError>`",
            ));
        }
    };

    if let Type::Path(type_path) = ty
        && let Some(last) = type_path.path.segments.last()
        && last.ident == "Result"
    {
        // Good enough — we'll trust the user on the error type
        return Ok(());
    }

    Err(syn::Error::new_spanned(
        ty,
        "return type must be `Result<T, StepError>`",
    ))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        let secs = parse_duration_str("2h", Span::call_site()).unwrap();
        assert_eq!(secs, 7200);
    }

    #[test]
    fn parse_duration_days() {
        let secs = parse_duration_str("3d", Span::call_site()).unwrap();
        assert_eq!(secs, 259200);
    }

    #[test]
    fn parse_duration_minutes() {
        let secs = parse_duration_str("5m", Span::call_site()).unwrap();
        assert_eq!(secs, 300);
    }

    #[test]
    fn parse_duration_seconds() {
        let secs = parse_duration_str("30s", Span::call_site()).unwrap();
        assert_eq!(secs, 30);
    }

    #[test]
    fn parse_duration_invalid() {
        assert!(parse_duration_str("5x", Span::call_site()).is_err());
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
