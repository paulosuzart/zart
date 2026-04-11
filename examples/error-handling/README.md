# Error Handling Example

Demonstrates the full spectrum of error handling in Zart durable executions.

## What this example shows

Zart distinguishes **business errors** (typed errors returned by your step logic) from **framework errors** (retry exhausted, timeout, deadline exceeded). This example shows four patterns for handling both:

| Pattern | API | When to use |
|---|---|---|
| Fail-fast | `step.await?` (the default `require()`) | Step failure should abort the execution immediately |
| Explicit three-way match | `zart::step(s).await?` + `StepOutcome` | You want to branch on Ok / typed business error / framework error |
| Inline fallback | `zart::step_or_else(s, \|e\| ...)` | You have a simple fallback value for business errors |
| Centralized recovery | `on_failure = handler` on `#[zart_durable]` | One place to compensate for any failure after propagation |

## Typed step errors

Each step declares its own error type:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
enum PaymentError {
    #[error("insufficient funds: balance {balance}, needed {needed}")]
    InsufficientFunds { balance: f64, needed: f64 },
    #[error("card declined: {reason}")]
    CardDeclined { reason: String },
}

#[zart_step("charge-card", retry = "fixed(2, 1s)")]
async fn charge_card(account_id: String, amount: f64) -> Result<PaymentResult, PaymentError> {
    // ...
}
```

`Serialize + Deserialize` are required so Zart can persist the error to the database and recover it later in `on_failure`.

## Pattern 1: fail-fast with `require()`

Using `await?` directly calls `require()` under the hood. Any non-Ok outcome (business error or framework error) immediately fails the execution and routes to `on_failure`.

```rust
let inventory = reserve_inventory(data.order_id.clone()).await?;
```

## Pattern 2: explicit three-way match with `zart::step()`

```rust
let balance = match zart::step(check_balance(data.account_id.clone())).await? {
    StepOutcome::Ok(b) => b,
    StepOutcome::BusinessErr(PaymentError::InsufficientFunds { balance, needed }) => {
        return Ok(OrderOutput { status: "insufficient_funds".to_string(), ... });
    }
    StepOutcome::BusinessErr(PaymentError::CardDeclined { reason }) => {
        return Ok(OrderOutput { status: "card_declined".to_string(), ... });
    }
    StepOutcome::ZartErr(e) => return Err(e.into()),
};
```

Use this when you need to handle specific business error variants inline and return a successful output for them.

## Pattern 3: inline fallback with `zart::step_or_else()`

```rust
let payment = zart::step_or_else(charge_card(data.account_id.clone(), data.amount), |e| {
    println!("Payment fallback triggered by: {e}");
    PaymentResult { transaction_id: "fallback".to_string(), amount: 0.0 }
})
.await?;
```

The closure receives the **typed** `PaymentError` and returns a fallback value. Framework errors (retry exhausted, timeout) are **not** intercepted — they still propagate as `Err`.

## Pattern 4: centralized `on_failure` handler

```rust
#[zart_durable("error-handling-demo", on_failure = handle_order_failure)]
async fn process_order(data: OrderInput) -> Result<OrderOutput, TaskError> { ... }

async fn handle_order_failure(
    data: OrderInput,
    failure: ExecutionFailure,
) -> Result<OrderOutput, TaskError> {
    match failure {
        ExecutionFailure::StepFailed { step, raw } if step == "charge-card" => {
            // Deserialize `raw` back to the typed error for precise handling:
            match serde_json::from_value::<PaymentError>(raw.clone()) {
                Ok(PaymentError::InsufficientFunds { balance, needed }) => { ... }
                Ok(PaymentError::CardDeclined { reason }) => { ... }
                Err(_) => { /* framework error — raw is not a PaymentError */ }
            }
        }
        ExecutionFailure::ExecutionDeadlineExceeded => { ... }
        ExecutionFailure::RetriesExhausted { attempts } => { ... }
        _ => Err(TaskError::Cancelled),
    }
}
```

`on_failure` is a plain `async fn` — no framework setup needed, fully unit-testable.

### Inline body vs `on_failure`: when to use which

| | Inline body (`zart::step` / `step_or_else`) | `on_failure` |
|---|---|---|
| **Scope** | A single step's outcome | Any step or execution-level failure |
| **Access to prior results** | Yes — all `let` bindings are in scope | No — handler receives only `data` and `failure` |
| **Can return a success output** | Yes | Yes |
| **Use when** | You have a simple fallback or need to branch on a specific error | You want a global safety net or compensation logic |

## How to run

```bash
just example-error-handling
```

Requires a running PostgreSQL instance. Start one with `just up`.
