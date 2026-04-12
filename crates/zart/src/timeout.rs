/// Timeout scope for step-level timeouts.
///
/// Controls whether a step's timeout is a **global deadline** (shared across all retry attempts)
/// or a **per-attempt countdown** (fresh countdown on each attempt).
///
/// # Examples
///
/// ```rust,ignore
/// // Global scope (default): 5 minutes total across all retries
/// #[zart_step("call-api", timeout = "5m", retry = "fixed(3, 1s)")]
///
/// // Per-attempt scope: each attempt gets a fresh 30 seconds
/// #[zart_step("call-api", timeout = "30s", timeout_scope = "per_attempt", retry = "fixed(3, 1s)")]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeoutScope {
    /// Deadline is calculated from the first attempt.
    ///
    /// `deadline = first_attempt_time + timeout_duration`
    /// All retry attempts share the same deadline. If the deadline has passed
    /// when a retry is picked up, the step immediately completes with `TimedOut`.
    ///
    /// This is the **default** behavior.
    #[default]
    Global,

    /// Each attempt gets a fresh countdown.
    ///
    /// No deadline is persisted — each attempt is wrapped in
    /// `tokio::time::timeout(step.timeout(), run())` independently.
    PerAttempt,
}

impl std::fmt::Display for TimeoutScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutScope::Global => write!(f, "global"),
            TimeoutScope::PerAttempt => write!(f, "per_attempt"),
        }
    }
}

impl std::str::FromStr for TimeoutScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "global" => Ok(TimeoutScope::Global),
            "per_attempt" => Ok(TimeoutScope::PerAttempt),
            _ => Err(format!(
                "invalid timeout_scope: '{s}'. Expected 'global' or 'per_attempt'"
            )),
        }
    }
}
