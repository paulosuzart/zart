# Transactions Example

Demonstrates Zart's **transaction participation** features for atomic scheduling and atomic step completion.

## Features Used

- **`start_in_tx` / `start_for_in_tx`** — atomically create business records and start a durable execution in the same database transaction (Gap 1)
- **`zart::trx`** — register a transaction inside a step's `run()` method so the framework uses it for atomic step completion (Scenario 2)

## Flow

1. **Scenario 1: Transactional Scheduling** — inserts a user record and starts a durable onboarding execution within a single `sqlx::Transaction`. If the transaction rolls back, neither the user nor the execution record exists.
2. **Scenario 2: Atomic Step Completion** — the `DeductBalanceStep` calls `zart::trx(&pool)` to register a transaction. The framework detects this after `run()` returns and uses the same transaction to persist step completion metadata, then commits. The user's `UPDATE` and the framework's `INSERT`/`UPDATE` are atomic.

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-transactions
```

## What You'll See

```
=== Zart Transaction Example ===

--- Scenario 1: Transactional Scheduling ---
Creating user and starting onboarding in a single transaction...

  User created:     user-<uuid>@example.com
  User ID:          <uuid>
  Initial balance:  1000
  Execution started atomically ✓

--- Running Worker (Scenario 2: zart::trx in deduct-balance step) ---

--- Results ---

  Execution completed ✓
  User:           user-<uuid>@example.com
  Final balance:  900
  (1000 initial - 100 bonus deduction = 900)

=== All checks passed ===
```

## Key Concepts

### Scenario 1: `start_in_tx` — Composable Scheduling

```rust
let mut tx = pool.begin().await?;

sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
    .bind(user_id).bind(&email)
    .execute(&mut *tx)
    .await?;

durable
    .start_in_tx(
        &mut tx,
        &format!("onboard-{user_id}"),
        "onboarding",
        serde_json::to_value(OnboardInput { user_id, email })?,
    )
    .await?;

tx.commit().await?;  // both commit atomically
```

If `tx` rolls back, no execution record or body task will exist — the caller's business record and the durable execution are always consistent.

### Scenario 2: `zart::trx` — Atomic Step Completion

```rust
#[async_trait::async_trait]
impl ZartStep for DeductBalanceStep {
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let pool = get_pool();
        let mut tx = zart::trx(pool).await?;

        let row: (i64,) = sqlx::query_as(
            "UPDATE demo_users SET balance = balance - $1 WHERE id = $2 RETURNING balance",
        )
        .bind(self.amount)
        .bind(self.user_id)
        .fetch_one(&mut **tx)
        .await?;

        Ok(row.0)
        // Framework detects the registered transaction, uses it for
        // step completion writes, then commits — all atomically.
    }
}
```

This eliminates the window where a crash between the step's DB write and the framework's completion writes could cause a duplicate side effect on retry.

## Contract

- **`start_in_tx`**: the caller is responsible for committing or rolling back the transaction. If the transaction rolls back, no execution record exists.
- **`zart::trx`**: must be called at most once per step invocation. Do **not** commit or roll back the returned transaction — the framework owns the lifecycle after `trx()` returns.
