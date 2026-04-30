# Spec 0048: Proper SQLx Enum Type Usage

## Problem

The codebase has several places where PostgreSQL enum types are cast to `::text` in SQL queries:

- `crates/zart/tests/integration/selective_rerun.rs:156`
- `examples/scheduler-only/src/main.rs:211`
- `crates/zart/tests/integration/admin_retry.rs:89`

Meanwhile, the core types (`TaskStatus`, `ExecutionStatus`, `StepStatus`, `StepResultKind`, `StepKind`, `StepAttemptStatus`, `ExecutionTrigger`) already have proper `#[derive(sqlx::Type)]` implementations with correct `#[sqlx(type_name = "...", rename_all = "snake_case")]` attributes in `crates/zart-core/src/types.rs`.

The main storage implementations (`step_storage_impl.rs`, `execution_storage_impl.rs`) correctly use these types without `::text` casts. The casts only appear in tests and examples where raw tuple types like `(String, String, Option<Value>)` are used instead of proper enum types.

## Current State

### PostgreSQL Enum Types (from migrations)

- `task_status` ('scheduled', 'picked_up', 'completed', 'failed', 'dead', 'cancelled')
- `step_result_kind` ('ok', 'err', 'rx', 'timeout', 'dl')
- `execution_status` ('scheduled', 'running', 'completed', 'failed', 'cancelled')
- `execution_trigger` ('initial', 'restart', 'selective_rerun')
- `step_status` ('scheduled', 'running', 'completed', 'dead')
- `step_kind` ('step', 'sleep', 'wait_all', 'wait_for_event', 'wait_group', 'capture')
- `step_attempt_status` ('completed', 'failed')

### Rust Types with SQLx Support

All types in `crates/zart-core/src/types.rs` already implement `sqlx::Type`:

```rust
#[derive(sqlx::Type)]
#[sqlx(type_name = "task_status", rename_all = "snake_case")]
pub enum TaskStatus { ... }

#[derive(sqlx::Type)]
#[sqlx(type_name = "execution_status", rename_all = "snake_case")]
pub enum ExecutionStatus { ... }

// etc.
```

## Proposal

### 1. Remove `::text` Casts in Tests and Examples

Replace raw string tuple queries with properly typed queries using SQLx enum support.

**Before:**
```rust
let rows: Vec<(String, String, Option<serde_json::Value>)> =
    sqlx::query_as("SELECT step_name, status::text, result FROM zart_steps WHERE run_id = $1")
        .bind(&run_id)
        .fetch_all(&pool)
        .await?;
```

**After:**
```rust
let rows: Vec<(String, StepStatus, Option<serde_json::Value>)> =
    sqlx::query_as("SELECT step_name, status, result FROM zart_steps WHERE run_id = $1")
        .bind(&run_id)
        .fetch_all(&pool)
        .await?;
```

### 2. Create Typed Query Helpers (Optional)

For tests and examples that need to inspect enum values as strings, consider creating helper functions or use pattern matching instead of SQL casts:

```rust
// Instead of casting to text in SQL, match on the enum in Rust
match status {
    StepStatus::Completed => "completed",
    StepStatus::Scheduled => "scheduled",
    // ...
}
```

## Implementation Plan

1. **Update test files** (`selective_rerun.rs`, `admin_retry.rs`):
   - Change tuple types to use proper enum types
   - Remove `::text` casts
   - Update assertions to match on enum variants instead of strings

2. **Update example** (`scheduler-only/src/main.rs`):
   - Change tuple types to use `TaskStatus` instead of `String`
   - Remove `::text` cast
   - Update string comparisons to use enum matching

3. **Verify with tests**:
   - Run `just fmt`, `just lint`
   - Run integration tests
   - Ensure all example tests pass

## Benefits

1. **Type safety**: Catch mismatches at compile time rather than runtime
2. **Consistency**: Align test/example code with the main storage implementation
3. **Performance**: Avoid unnecessary text casts in PostgreSQL
4. **Documentation**: Demonstrates proper SQLx enum usage for future contributors

## Compatibility

This change is backward-compatible as it only affects how Rust code queries the database, not the database schema itself. The PostgreSQL enum types remain unchanged.
