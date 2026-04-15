//! Error Handling Example
//!
//! Demonstrates the full spectrum of zart error handling:
//!
//! 1. **`zart::require()`** — default fail-fast; any non-Ok outcome fails the execution
//! 2. **`zart::step()` + `StepOutcome`** — explicit three-way matching on Ok / BusinessErr / ZartErr
//! 3. **`zart::step_or_else()`** — inline fallback on business errors only
//! 4. **`on_failure`** — centralized recovery handler for any propagated failure
//!
//! Key concept: step errors are **typed**. Each step declares its own `Error` type,
//! and the framework distinguishes business errors (the step's own failure) from
//! framework errors (retry exhausted, timeout, deadline exceeded).

use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;
use zart::error::{ExecutionFailure, StepOutcome, TaskError};
use zart::prelude::*;
use zart::{zart_durable, zart_step};
use zart_scheduler::PostgresScheduler;

// ── Step error types ──────────────────────────────────────────────────────────

/// Errors from the payment step — these are business decisions.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
enum PaymentError {
    #[error("insufficient funds: balance {balance}, needed {needed}")]
    InsufficientFunds { balance: f64, needed: f64 },
    #[error("card declined: {reason}")]
    CardDeclined { reason: String },
}

/// Errors from the inventory step.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
enum InventoryError {
    #[error("item out of stock: {item}")]
    OutOfStock { item: String },
    #[error("item reserved by another order")]
    Reserved,
}

// ── Step results ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryResult {
    item: String,
    quantity: u32,
}

// ── Step definitions ──────────────────────────────────────────────────────────

/// Step that may fail with insufficient funds — demonstrates explicit matching.
#[zart_step("check-balance")]
async fn check_balance(_account_id: String) -> Result<f64, PaymentError> {
    // Simulated: 30% chance of low balance.
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    if (seed % 10) < 3 {
        return Err(PaymentError::InsufficientFunds {
            balance: 5.0,
            needed: 50.0,
        });
    }
    Ok(100.0)
}

/// Step that charges a card — demonstrates `step_or_else` fallback.
#[zart_step("charge-card", retry = "fixed(2, 1s)")]
async fn charge_card(_account_id: String, amount: f64) -> Result<PaymentResult, PaymentError> {
    // Simulated: first attempt fails, retry succeeds.
    if zart::context().current_attempt == 0 {
        return Err(PaymentError::CardDeclined {
            reason: "temporary network error".to_string(),
        });
    }
    Ok(PaymentResult {
        transaction_id: format!("txn-{amount}"),
        amount,
    })
}

/// Step that reserves inventory — demonstrates `require()` fail-fast.
#[zart_step("reserve-inventory")]
async fn reserve_inventory(_order_id: String) -> Result<InventoryResult, InventoryError> {
    Ok(InventoryResult {
        item: "widget".to_string(),
        quantity: 1,
    })
}

// ── Order processing input / output ───────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    account_id: String,
    order_id: String,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderOutput {
    status: String,
    transaction_id: Option<String>,
    balance: Option<f64>,
    inventory_item: Option<String>,
    message: String,
}

// ── Centralized failure handler ───────────────────────────────────────────────

/// Called when `run` returns `Err` or an execution-level failure occurs.
///
/// This function is a plain async fn — fully unit-testable without any framework setup.
/// The macro generates a delegation call.
async fn handle_order_failure(
    data: OrderInput,
    failure: ExecutionFailure,
) -> Result<OrderOutput, TaskError> {
    eprintln!("[on_failure] invoked for account={}", data.account_id);

    match failure {
        ExecutionFailure::StepFailed { step, raw } if step == "charge-card" => {
            // Deserialize `raw` back into the step's typed error for precise matching.
            // raw is the JSON-serialized PaymentError that the step returned.
            match serde_json::from_value::<PaymentError>(raw.clone()) {
                Ok(PaymentError::InsufficientFunds { balance, needed }) => {
                    eprintln!("[on_failure] insufficient funds: have ${balance}, need ${needed}");
                    Ok(OrderOutput {
                        status: "payment_failed".to_string(),
                        transaction_id: None,
                        balance: Some(balance),
                        inventory_item: None,
                        message: format!("Insufficient funds: have ${balance}, need ${needed}"),
                    })
                }
                Ok(PaymentError::CardDeclined { reason }) => {
                    eprintln!("[on_failure] card declined: {reason}");
                    Ok(OrderOutput {
                        status: "payment_failed".to_string(),
                        transaction_id: None,
                        balance: None,
                        inventory_item: None,
                        message: format!("Card declined: {reason}"),
                    })
                }
                Err(_) => {
                    // Framework error (retry exhausted, timeout) — raw may not be a PaymentError.
                    eprintln!("[on_failure] payment step framework error: {raw}");
                    Ok(OrderOutput {
                        status: "payment_failed".to_string(),
                        transaction_id: None,
                        balance: None,
                        inventory_item: None,
                        message: format!("Payment step failed: {raw}"),
                    })
                }
            }
        }
        ExecutionFailure::StepFailed { step, raw } if step == "reserve-inventory" => {
            // Same pattern: deserialize raw to InventoryError for specific handling.
            match serde_json::from_value::<InventoryError>(raw.clone()) {
                Ok(InventoryError::OutOfStock { item }) => {
                    eprintln!("[on_failure] out of stock: {item}");
                    Ok(OrderOutput {
                        status: "inventory_failed".to_string(),
                        transaction_id: None,
                        balance: None,
                        inventory_item: None,
                        message: format!("Item out of stock: {item}"),
                    })
                }
                Ok(InventoryError::Reserved) => {
                    eprintln!("[on_failure] item reserved by another order");
                    Ok(OrderOutput {
                        status: "inventory_failed".to_string(),
                        transaction_id: None,
                        balance: None,
                        inventory_item: None,
                        message: "Item reserved by another order".to_string(),
                    })
                }
                Err(_) => {
                    eprintln!("[on_failure] inventory step framework error: {raw}");
                    Ok(OrderOutput {
                        status: "inventory_failed".to_string(),
                        transaction_id: None,
                        balance: None,
                        inventory_item: None,
                        message: format!("Inventory step failed: {raw}"),
                    })
                }
            }
        }
        ExecutionFailure::StepFailed { step, raw } => {
            eprintln!("[on_failure] unknown step failure: {step}: {raw}");
            Err(TaskError::Cancelled)
        }
        ExecutionFailure::ExecutionDeadlineExceeded => {
            eprintln!("[on_failure] execution deadline exceeded");
            Ok(OrderOutput {
                status: "timed_out".to_string(),
                transaction_id: None,
                balance: None,
                inventory_item: None,
                message: "Execution timed out".to_string(),
            })
        }
        ExecutionFailure::RetriesExhausted { attempts } => {
            eprintln!("[on_failure] retries exhausted after {attempts} attempts");
            Ok(OrderOutput {
                status: "retries_exhausted".to_string(),
                transaction_id: None,
                balance: None,
                inventory_item: None,
                message: format!("Execution gave up after {attempts} attempts"),
            })
        }
    }
}

// ── Durable execution handler ─────────────────────────────────────────────────

#[zart_durable("error-handling-demo", on_failure = handle_order_failure)]
async fn process_order(data: OrderInput) -> Result<OrderOutput, TaskError> {
    // ── 1. require() — fail-fast on any non-Ok outcome ────────────────────────
    let inventory = reserve_inventory(data.order_id.clone()).await?;
    println!(
        "[process_order] Reserved: {} x{}",
        inventory.item, inventory.quantity
    );

    // ── 2. zart::step() — branch on specific business errors ─────────────────
    let balance = match zart::step(check_balance(data.account_id.clone())).await? {
        StepOutcome::Ok(b) => {
            println!("[process_order] Balance check: ${b}");
            b
        }
        StepOutcome::BusinessErr(PaymentError::InsufficientFunds { balance, needed }) => {
            return Ok(OrderOutput {
                status: "insufficient_funds".to_string(),
                transaction_id: None,
                balance: Some(balance),
                inventory_item: Some(inventory.item),
                message: format!("Insufficient funds: have ${balance}, need ${needed}"),
            });
        }
        StepOutcome::BusinessErr(PaymentError::CardDeclined { reason }) => {
            return Ok(OrderOutput {
                status: "card_declined".to_string(),
                transaction_id: None,
                balance: None,
                inventory_item: Some(inventory.item),
                message: format!("Card declined: {reason}"),
            });
        }
        // Everything else falls through to on_failure.
        StepOutcome::ZartErr(e) => return Err(e.into()),
    };

    // ── 3. step_or_else() — inline fallback on business error only ────────────
    let payment = zart::step_or_else(charge_card(data.account_id.clone(), data.amount), |e| {
        // This closure receives PaymentError — framework errors still propagate.
        println!("[process_order] Payment fallback: {e}");
        PaymentResult {
            transaction_id: "fallback".to_string(),
            amount: 0.0,
        }
    })
    .await?;

    println!(
        "[process_order] Payment: {} (${})",
        payment.transaction_id, payment.amount
    );

    Ok(OrderOutput {
        status: "completed".to_string(),
        transaction_id: Some(payment.transaction_id),
        balance: Some(balance),
        inventory_item: Some(inventory.item),
        message: "Order processed successfully".to_string(),
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Error Handling Example ===\n");
    println!("Demonstrates:");
    println!("  1. zart::require() — fail-fast semantics");
    println!("  2. zart::step() + StepOutcome — explicit error matching");
    println!("  3. zart::step_or_else() — inline fallback on business errors");
    println!("  4. on_failure — centralized recovery handler\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = std::sync::Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("error-handling-demo", ProcessOrder);
    let registry = std::sync::Arc::new(registry);

    let execution_id = format!("error-handling-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = OrderInput {
        account_id: "acct-123".to_string(),
        order_id: "order-456".to_string(),
        amount: 50.0,
    };

    println!("Starting execution '{}'...\n", execution_id);
    durable
        .start_for::<ProcessOrder>(&execution_id, "error-handling-demo", &input)
        .await?;

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(10),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = std::sync::Arc::new(zart::Worker::new(sched.clone(), registry.clone(), config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let output: OrderOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(30), None)
        .await?;

    worker.stop();

    println!("\n=== Execution Completed ===");
    println!("  Status:       {}", output.status);
    println!("  Transaction:  {:?}", output.transaction_id);
    println!("  Balance:      {:?}", output.balance);
    println!("  Inventory:    {:?}", output.inventory_item);
    println!("  Message:      {}", output.message);

    Ok(())
}
