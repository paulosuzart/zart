---
title: Python Examples
description: Walkthroughs of the runnable Python examples in bindings/python/examples/.
---

All four examples live in `bindings/python/examples/` and run against PostgreSQL (`DATABASE_URL=postgres://zart:zart@localhost:5432/zart`).

From `bindings/python/`:

```bash
uv sync --all-extras
uv run maturin develop
```

## 1. Transactional Scheduling & Atomic Steps (`transactions.py`)

Demonstrates `async with await backend.begin() as tx:` + `scheduler.start_in_tx(...)` to atomically insert a `demo_users` row (`balance=1000`) and schedule `onboard-user`, followed by `@zart.step` functions using `tx: zart.Trx = zart.trx()` to deduct `100` (`final_balance=900`) and mark `onboarded = TRUE`.

```bash
uv run python examples/transactions.py
```

## 2. Human-in-the-Loop Approval (`approval_workflow.py`)

Demonstrates typed `dataclass` payloads (`ApprovalRequest`, `ApprovalDecision`, `ApprovalOutput`), `await validate_request(data)`, suspending on `await zart.wait_for_event("manager-approval", timeout="2m", target_type=ApprovalDecision)`, and resuming via `await scheduler.offer_event(...)`.

```bash
uv run python examples/approval_workflow.py
```

## 3. Parallel Saga & Durable Loops (`parallel_saga.py`)

Demonstrates a multi-step order fulfillment saga combining:
- Direct `await validate_order(order)`
- Durable loop with `await process_line_item(item).named(f"item-{i}")` and `Retry.fixed(2, "20ms")`
- Durable `await zart.sleep("dispatch-cooldown", "20ms")`
- Parallel fan-out/fan-in via `zart.schedule(...)` + `await zart.wait([...])`
- Graceful fallback via `await zart.step_or(fetch_promo_discount(...), 0.0)`

```bash
uv run python examples/parallel_saga.py
```

## 4. SQLAlchemy Checkout & Retry Rollback (`sqlalchemy_checkout.py`)

Demonstrates declarative SQLAlchemy ORM models (`Account`, `CheckoutOrder`) with `session: AsyncSession = zart.trx(AsyncSessionLocal)`, direct `await reserve_inventory(...)` / `await charge_account(...)` / `await finalize_order(...)`, and `Retry.fixed(2, "50ms")` showing automatic `rollback()` on a transient failure followed by `flush()` + `commit()` on retry.

```bash
uv run python examples/sqlalchemy_checkout.py
```
