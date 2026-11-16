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

## Running Examples

Make sure PostgreSQL is running (or set `DATABASE_URL`):

```bash
export DATABASE_URL="postgres://zart:zart@localhost:5432/zart"

# Transactional scheduling (start_in_tx) and atomic step transactions (zart.trx)
uv run python examples/transactions.py

# Human-in-the-loop approval workflow (wait_for_event + offer_event)
uv run python examples/approval_workflow.py
```

## Running Tests

```bash
uv run pytest -v
```
