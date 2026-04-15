//! HTTP server setup and lifecycle management.

use crate::routes;
use crate::state::AppState;
use axum::Router;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use zart::DurableApi;

/// The Zart API server.
///
/// Wraps an Axum router and exposes durable execution management over HTTP.
pub struct ApiServer {
    /// TCP address to bind, e.g. `"0.0.0.0:8080"`.
    addr: String,
    /// The durable execution backend.
    durable: Arc<dyn DurableApi>,
    /// CancellationToken for graceful shutdown.
    cancellation: Option<CancellationToken>,
}

impl ApiServer {
    /// Create a new API server bound to `addr`.
    pub fn new(addr: impl Into<String>, durable: Arc<dyn DurableApi>) -> Self {
        Self {
            addr: addr.into(),
            durable,
            cancellation: None,
        }
    }

    /// Create a new API server with a cancellation token for graceful shutdown.
    pub fn with_cancellation(
        addr: impl Into<String>,
        durable: Arc<dyn DurableApi>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            addr: addr.into(),
            durable,
            cancellation: Some(cancellation),
        }
    }

    /// Build the Axum router with all API routes and middleware.
    pub fn router(&self) -> Router {
        let state = AppState::new(self.durable.clone());
        routes::api_router(state)
            .layer(TraceLayer::new_for_http())
            .layer(CorsLayer::permissive())
    }

    /// Start listening and serving requests.
    ///
    /// Blocks until the server is shut down.
    pub async fn serve(self) -> Result<(), std::io::Error> {
        info!(addr = %self.addr, "Zart API server starting");
        let router = self.router();
        let listener = tokio::net::TcpListener::bind(&self.addr).await?;

        // Move cancellation token into a variable we can control
        let cancellation = self.cancellation;

        if let Some(cancellation) = cancellation {
            info!("Zart API server configured with graceful shutdown");
            // Create the shutdown signal future and keep cancellation alive
            let shutdown_signal = async move {
                cancellation.cancelled().await;
            };
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal)
                .await
        } else {
            axum::serve(listener, router).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::time::Duration;
    use zart::error::SchedulerError;
    use zart_scheduler::ListExecutionsParams;
    use zart_scheduler::{ExecutionRecord, ExecutionStats, ScheduleResult};

    struct NullApi;

    #[async_trait]
    impl DurableApi for NullApi {
        async fn start(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<ScheduleResult, SchedulerError> {
            unimplemented!()
        }
        async fn cancel(&self, _: &str) -> Result<bool, SchedulerError> {
            unimplemented!()
        }
        async fn status(&self, _: &str) -> Result<ExecutionRecord, SchedulerError> {
            unimplemented!()
        }
        async fn wait(
            &self,
            _: &str,
            _: Duration,
            _: Option<Duration>,
        ) -> Result<ExecutionRecord, SchedulerError> {
            unimplemented!()
        }
        async fn offer_event(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), SchedulerError> {
            unimplemented!()
        }
        async fn list_executions(
            &self,
            _: ListExecutionsParams,
        ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
            unimplemented!()
        }
        async fn stats(&self) -> Result<ExecutionStats, SchedulerError> {
            Ok(ExecutionStats::default())
        }
    }

    #[test]
    fn server_builds_router() {
        let server = ApiServer::new("0.0.0.0:8080", Arc::new(NullApi));
        let _ = server.router();
    }
}
