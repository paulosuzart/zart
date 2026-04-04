//! Zart HTTP API — optional Axum server for external interaction with durable executions.
//!
//! # Endpoints (M5)
//!
//! ```text
//! GET    /api/v1/executions                        — List executions
//! POST   /api/v1/executions                        — Start a new execution
//! GET    /api/v1/executions/:execution_id          — Get execution status
//! POST   /api/v1/executions/:execution_id/cancel   — Cancel an execution
//! GET    /api/v1/executions/:execution_id/wait     — Long-poll until completion
//! POST   /api/v1/events/:execution_id/:event_name  — Deliver an event
//! ```
//!
//! # Usage (M5)
//!
//! ```rust,no_run
//! use zart_api::ApiServer;
//! use std::sync::Arc;
//!
//! # async fn example() {
//! // let server = ApiServer::new(scheduler, registry);
//! // server.serve("0.0.0.0:8080").await.unwrap();
//! # }
//! ```

pub mod routes;
pub mod server;

pub use server::ApiServer;
