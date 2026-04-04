//! Retry configuration for steps and durable executions.

use std::time::Duration;

/// Policy controlling how a step or execution is retried on failure.
///
/// # Examples
///
/// ```rust
/// use zart::retry::RetryConfig;
/// use std::time::Duration;
///
/// // No retries (default).
/// let none = RetryConfig::none();
///
/// // Retry 3 times with a fixed 5-second delay between attempts.
/// let fixed = RetryConfig::fixed(3, Duration::from_secs(5));
///
/// // Retry 5 times with exponential backoff starting at 1 second.
/// let exp = RetryConfig::exponential(5, Duration::from_secs(1));
/// ```
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (does not count the initial attempt).
    ///
    /// `0` means no retries — the step fails immediately on first error.
    pub max_attempts: usize,

    /// Delay before the first retry.
    pub initial_delay: Duration,

    /// Multiplier applied to `initial_delay` after each retry.
    ///
    /// `1.0` produces fixed-interval retries. `2.0` doubles the delay each time.
    pub backoff_multiplier: f64,

    /// Optional cap on the computed delay.
    ///
    /// Without a cap, exponential backoff can produce arbitrarily large delays.
    pub max_delay: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            initial_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            max_delay: None,
        }
    }
}

impl RetryConfig {
    /// No retries — the first failure is terminal.
    pub fn none() -> Self {
        Self {
            max_attempts: 0,
            ..Default::default()
        }
    }

    /// Retry `attempts` times with a constant `delay` between each attempt.
    pub fn fixed(attempts: usize, delay: Duration) -> Self {
        Self {
            max_attempts: attempts,
            initial_delay: delay,
            backoff_multiplier: 1.0,
            max_delay: None,
        }
    }

    /// Retry `attempts` times with exponential backoff starting at `initial_delay`.
    ///
    /// The delay doubles after each failed attempt (multiplier = 2.0).
    pub fn exponential(attempts: usize, initial_delay: Duration) -> Self {
        Self {
            max_attempts: attempts,
            initial_delay,
            backoff_multiplier: 2.0,
            max_delay: None,
        }
    }

    /// Compute the delay before attempt number `attempt_number` (1-indexed).
    ///
    /// Returns `None` if `attempt_number > max_attempts`.
    pub fn delay_for(&self, attempt_number: usize) -> Option<Duration> {
        if attempt_number == 0 || attempt_number > self.max_attempts {
            return None;
        }
        let multiplier = self.backoff_multiplier.powi((attempt_number - 1) as i32);
        let millis = (self.initial_delay.as_millis() as f64 * multiplier) as u64;
        let delay = Duration::from_millis(millis);
        Some(match self.max_delay {
            Some(cap) => delay.min(cap),
            None => delay,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_delay_is_constant() {
        let cfg = RetryConfig::fixed(3, Duration::from_secs(5));
        assert_eq!(cfg.delay_for(1), Some(Duration::from_secs(5)));
        assert_eq!(cfg.delay_for(2), Some(Duration::from_secs(5)));
        assert_eq!(cfg.delay_for(3), Some(Duration::from_secs(5)));
        assert_eq!(cfg.delay_for(4), None); // beyond max_attempts
    }

    #[test]
    fn exponential_delay_doubles() {
        let cfg = RetryConfig::exponential(3, Duration::from_secs(1));
        assert_eq!(cfg.delay_for(1), Some(Duration::from_secs(1)));
        assert_eq!(cfg.delay_for(2), Some(Duration::from_secs(2)));
        assert_eq!(cfg.delay_for(3), Some(Duration::from_secs(4)));
    }

    #[test]
    fn max_delay_cap_is_respected() {
        let cfg = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            max_delay: Some(Duration::from_secs(5)),
        };
        assert_eq!(cfg.delay_for(4), Some(Duration::from_secs(5)));
        assert_eq!(cfg.delay_for(5), Some(Duration::from_secs(5)));
    }

    #[test]
    fn no_retry_returns_none_for_any_attempt() {
        let cfg = RetryConfig::none();
        assert_eq!(cfg.delay_for(1), None);
    }
}
