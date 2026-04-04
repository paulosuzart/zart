//! HTTP server setup and lifecycle management.

use crate::routes;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// The Zart API server.
///
/// Wraps an Axum router and exposes durable execution management over HTTP.
/// Fully implemented in M5.
pub struct ApiServer {
    /// TCP address to bind, e.g. `"0.0.0.0:8080"`.
    addr: String,
}

impl ApiServer {
    /// Create a new API server bound to `addr`.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// Build the Axum router with all API routes and middleware.
    pub fn router(&self) -> Router {
        routes::api_router()
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
        axum::serve(listener, router).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_builds_router() {
        let server = ApiServer::new("0.0.0.0:8080");
        // Just ensure the router can be constructed without panicking.
        let _ = server.router();
    }
}
