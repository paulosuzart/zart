---
title: Python Transactions
description: Atomic step transactions and transactional scheduling in Python.
---

Zart's Python bindings support both **transactional scheduling** (`start_in_tx`) and **declarative transaction injection** inside `@zart.step` functions (`zart.trx()`).

## Transactional Scheduling (`start_in_tx`)

Commit your application's database write and the durable execution record in a single PostgreSQL transaction:

```python
async with await backend.begin() as tx:
    await tx.execute(
        "INSERT INTO demo_users (id, email, balance) VALUES ($1, $2, 1000)",
        user_id,
        email,
    )
    await scheduler.start_in_tx(
        tx,
        f"onboard-{user_id}",
        "onboard-user",
        zart.to_json_value(OnboardInput(user_id=user_id, email=email)),
    )
# Auto-commits on clean exit; auto-rolls back on exception
```

## Zart-Managed Step Transactions (`tx: zart.Trx = zart.trx()`)

When a `@zart.step` declares `tx: zart.Trx = zart.trx()` (or simply `tx: zart.Trx` without a default), Zart opens a `ZartTrx` and passes `tx` into your step automatically (callers omit `tx` when calling the step). Your SQL write and Zart's step completion record commit atomically:

```python
@zart.step("deduct-balance")
async def deduct_balance(user_id: UUID, amount: int, tx: zart.Trx = zart.trx()) -> int:
    row = await tx.fetch_one(
        """
        UPDATE demo_users
        SET balance = balance - $1
        WHERE id = $2
        RETURNING balance
        """,
        amount,
        user_id,
    )
    return int(row["balance"])

@zart.durable("onboard-user")
async def onboard_user(data: OnboardInput) -> int:
    return await deduct_balance(data.user_id, 100)
```

`Trx` and `ClientTrx` expose `execute(sql, *args)`, `fetch_one(sql, *args)`, `fetch_optional(sql, *args)`, and `fetch_all(sql, *args)` with dynamic parameter binding for `None`, `bool`, `int`, `float`, `str`, `bytes`, `UUID`, `datetime`, and JSON values.

## User-Managed Providers (SQLAlchemy & psycopg)

Pass any session/connection provider directly to `zart.trx(provider)` — or configure a worker-wide default with `WorkerBuilder(backend).trx_provider(provider)`:

### SQLAlchemy `AsyncSession`

```python
@zart.step("charge-account", retry=zart.Retry.fixed(2, "50ms"))
async def charge_account(
    order_id: str,
    amount: int,
    session: AsyncSession = zart.trx(AsyncSessionLocal),
) -> int:
    account = await session.get(Account, order_id)
    account.balance -= amount
    return account.balance
```

On success, Zart calls `await session.flush()`, `await session.commit()`, and `await session.close()`. On exception, it calls `await session.rollback()` and `await session.close()`.

### `psycopg` Connection Pool

```python
@zart.step("update-ledger")
async def update_ledger(
    entry_id: str,
    conn = zart.trx(pool.connection),
) -> None:
    await conn.execute("INSERT INTO ledger (id) VALUES (%s)", (entry_id,))
```

On success, Zart calls `await conn.commit()`; on exception, `await conn.rollback()`, before exiting the pool context manager.

### Worker-Level Default Provider

```python
worker = (
    zart.WorkerBuilder(backend)
    .trx_provider(AsyncSessionLocal)
    .register_durable(checkout_workflow)
    .build()
)
```

Any `@zart.step` parameter using `= zart.trx()` without an explicit `zart.Trx` annotation inherits `AsyncSessionLocal`, while parameters explicitly annotated `tx: zart.Trx = zart.trx()` continue to use Zart-managed transactions.
