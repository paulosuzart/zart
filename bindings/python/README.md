# Zart Python Bindings

Python 3.15 bindings for the [Zart](../../README.md) durable execution framework, powered by PyO3 and Tokio.

## Prerequisites

- **Python 3.15**
- **[uv](https://docs.astral.sh/uv/)** (or `maturin`)
- **Rust toolchain** (stable)
- **PostgreSQL** running locally (default connection string: `postgres://zart:zart@localhost:5432/zart`)

## Build & Install (Development)

From `bindings/python/`:

```bash
uv sync --all-extras
uv run maturin develop
```

## Quickstart

```python
import zart

@zart.step("charge-card", retry=zart.Retry.fixed(3, "1s"))
async def charge_card(order_id: str, amount: float) -> str:
    return f"charged:{order_id}:{amount}"

@zart.durable("checkout")
async def checkout(order_id: str) -> str:
    # Direct await shorthand (default DBOS-style path for required steps)
    receipt = await charge_card(order_id, 49.99)
    return receipt
```

### Step Execution Patterns

| Pattern | When to use |
|---|---|
| `await my_step(...)` | **Default** for required sequential steps — returns the typed result or raises on failure. |
| `await zart.step(my_step(...))` | When you want to inspect the `StepOutcome` (`is_ok`, `is_business_err`, `is_zart_err`) without raising. |
| `await zart.step_or(my_step(...), default)` / `await zart.step_or_else(my_step(...), fallback)` | When a step failure should produce a fallback value instead of failing the workflow. |
| `zart.schedule(my_step(...))` + `await zart.wait([...])` | Parallel fan-out / fan-in across multiple independent steps. |
| `await zart.require(my_step(...))` | Explicit equivalent of `await my_step(...)` (optional; never needed for normal sequential steps). |

## Running Examples

Make sure PostgreSQL is running (or set `DATABASE_URL`):

```bash
export DATABASE_URL="postgres://zart:zart@localhost:5432/zart"

# Transactional scheduling (start_in_tx) and atomic step transactions (zart.trx)
uv run python examples/transactions.py

# Human-in-the-loop approval workflow (wait_for_event + offer_event)
uv run python examples/approval_workflow.py

# Parallel saga with durable loops, schedule/wait fan-out, step_or fallback, and sleep
uv run python examples/parallel_saga.py

# E-commerce checkout with User-Managed SQLAlchemy AsyncSession & retry rollback
uv run python examples/sqlalchemy_checkout.py
```

## Running Tests

```bash
uv run pytest -v
```
