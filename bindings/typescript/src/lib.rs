//! N-API bindings for Zart (TypeScript / Effect).
#![deny(clippy::all)]

mod api;
mod backend;
mod durable;
mod errors;
mod scheduler;
mod serde_bridge;
mod step;
mod trx;
mod worker;

pub use api::{Dispatcher, ExecutionInfo};
pub use backend::PgBackend;
pub use errors::{StepOutcome, TsStepError};
pub use scheduler::{DurableScheduler, ExecutionRecord, ScheduleResult};
pub use step::{RetrySpec, StepSpec};
pub use trx::{ClientTrx, NativeTrx};
pub use worker::{Worker, WorkerBuilder};
