/// Render a `Debug` value as a lowercase string (e.g. `Running` → `"running"`).
pub fn fmt_lower<T: std::fmt::Debug>(v: &T) -> String {
    format!("{v:?}").to_lowercase()
}

/// Render an `Option<T>` as its string value or `"-"` when absent.
pub fn fmt_opt<T: ToString>(v: Option<T>) -> String {
    v.map(|t| t.to_string()).unwrap_or_else(|| "-".to_string())
}
