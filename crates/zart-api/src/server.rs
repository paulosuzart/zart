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
    /// Whether to mount Swagger UI (`/swagger-ui`) and schema (`/openapi.json`).
    #[cfg(feature = "openapi")]
    swagger_ui: bool,
}

impl ApiServer {
    /// Create a new API server bound to `addr`.
    pub fn new(addr: impl Into<String>, durable: Arc<dyn DurableApi>) -> Self {
        Self {
            addr: addr.into(),
            durable,
            cancellation: None,
            #[cfg(feature = "openapi")]
            swagger_ui: false,
        }
    }

    /// Create a new API server with a cancellation token for graceful shutdown.
    #[must_use]
    pub fn with_cancellation(
        addr: impl Into<String>,
        durable: Arc<dyn DurableApi>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            addr: addr.into(),
            durable,
            cancellation: Some(cancellation),
            #[cfg(feature = "openapi")]
            swagger_ui: false,
        }
    }

    /// Mount Swagger UI at `/swagger-ui` and serve the OpenAPI schema at `/openapi.json`.
    ///
    /// Only available with the `openapi` feature.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn with_swagger_ui(mut self) -> Self {
        self.swagger_ui = true;
        self
    }

    /// Build the Axum router with Main API routes and middleware.
    ///
    /// The Admin API router (`/zart/admin/v1/*`) is not included here; mount it
    /// separately via [`crate::admin_routes::admin_router`] if needed.
    pub fn router(&self) -> Router {
        let state = AppState::new(self.durable.clone());
        #[allow(unused_mut)]
        let mut app = routes::api_router(state);

        #[cfg(feature = "openapi")]
        if self.swagger_ui {
            use crate::openapi::ZartApiDoc;
            use utoipa::OpenApi as _;
            use utoipa_swagger_ui::SwaggerUi;
            app = app
                .merge(SwaggerUi::new("/swagger-ui").url("/openapi.json", ZartApiDoc::openapi()));
        }

        app.layer(TraceLayer::new_for_http())
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
    use zart::{ExecutionRecord, ExecutionStats, ListExecutionsParams};
    use zart_scheduler::ScheduleResult;

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

    #[cfg(feature = "openapi")]
    #[test]
    fn with_swagger_ui_builds_router() {
        let server = ApiServer::new("0.0.0.0:8080", Arc::new(NullApi)).with_swagger_ui();
        let _ = server.router();
    }
}
