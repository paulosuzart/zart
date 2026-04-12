//! Shared Axum application state.

use std::sync::Arc;
use zart::DurableApi;
use zart::DurableScheduler;

/// State injected into every route handler via Axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub durable: Arc<dyn DurableApi>,
}

impl AppState {
    pub fn new(durable: Arc<dyn DurableApi>) -> Self {
        Self { durable }
    }
}

/// State for admin route handlers.
///
/// Requires a concrete `DurableScheduler` because admin operations use
/// concrete return types (not object-safe trait methods).
#[derive(Clone)]
pub struct AdminState {
    pub scheduler: Arc<DurableScheduler>,
}
