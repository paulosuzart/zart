//! Shared Axum application state.

use std::sync::Arc;
use zart::DurableApi;

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
