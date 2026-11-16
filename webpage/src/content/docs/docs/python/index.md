---
title: Python Bindings
description: Durable execution for Python 3.15+ powered by Zart's Rust runtime.
---

Zart's Python bindings bring durable, crash-proof workflows to Python 3.15+ via a native PyO3 extension backed by Zart's Rust + PostgreSQL runtime.

## Prerequisites & Installation

- **Python 3.15+**
- **[uv](https://docs.astral.sh/uv/)** (or `maturin`)
- **Rust toolchain** (stable)
- **PostgreSQL** (default `DATABASE_URL`: `postgres://zart:zart@localhost:5432/zart`)

From `bindings/python/`:

```bash
uv sync --all-extras
uv run maturin develop
```

## Quickstart

```python
import asyncio
from dataclasses import dataclass
from uuid import UUID, uuid4
import zart

@dataclass
class OnboardingInput:
    user_id: UUID
    email: str

@zart.step("send-welcome", retry=zart.Retry.exponential(3, "1s"))
async def send_welcome(email: str) -> str:
    return f"sent:{email}"

@zart.step("provision-workspace")
async def provision_workspace(user_id: UUID) -> str:
    return f"ws:{user_id}"

@zart.durable("onboard-user", timeout="5m")
async def onboard_user(data: OnboardingInput) -> dict[str, str]:
    # Clean DBOS-style direct await — no wrapper needed
    welcome = await send_welcome(data.email)
    workspace = await provision_workspace(data.user_id)
    return {"welcome": welcome, "workspace": workspace}

async def main() -> None:
    backend = await zart.PgBackend.connect("postgres://zart:zart@localhost:5432/zart")
    await backend.run_migrations()

    scheduler = zart.DurableScheduler(backend)
    worker = zart.WorkerBuilder(backend).register_durable(onboard_user).build()
    worker_task = asyncio.ensure_future(worker.run())

    try:
        exec_id = f"onboard-{uuid4()}"
        payload = zart.to_json_value(OnboardingInput(user_id=uuid4(), email="ada@example.com"))
        await scheduler.start(exec_id, "onboard-user", payload)
        result = await scheduler.wait_completion(exec_id, timeout_secs=30.0)
        print("Completed:", result)
    finally:
        worker.stop()
        await worker_task
        await backend.close()

asyncio.run(main())
```

## Step Execution Patterns

| Pattern | When to use |
|---|---|
| `await my_step(...)` | **Default** for required sequential steps — returns the typed result or raises on failure. |
| `await my_step(...).named("item-0")` | Inside loops or dynamic branches where each invocation needs a distinct durable step ID. |
| `await zart.step(my_step(...))` | When you want to inspect the `StepOutcome` (`is_ok`, `is_business_err`, `is_zart_err`) without raising. |
| `await zart.step_or(my_step(...), default)` / `await zart.step_or_else(my_step(...), fallback)` | When a step failure should produce a fallback value instead of failing the workflow. |
| `zart.schedule(my_step(...))` + `await zart.wait([...])` | Parallel fan-out / fan-in across multiple independent steps, preserving handle order. |
| `await zart.require(my_step(...))` | Explicit equivalent of `await my_step(...)` (optional; never needed for normal sequential steps). |

## Control Flow, Timers & Events

- `await zart.sleep("cooldown", "5m")` — Durable sleep (accepts seconds, `timedelta`, or `"500ms"` / `"1s"` / `"5m"` / `"2h"` / `"1d"`).
- `await zart.sleep_until("cutoff", wake_datetime)` — Durable sleep until an RFC 3339 / `datetime` timestamp (naive datetimes are normalized to UTC).
- `await zart.wait_for_event("manager-approval", timeout="2m", target_type=ApprovalDecision)` — Suspend until an external event is delivered via `scheduler.offer_event(...)`.
- `await zart.capture("random-seed", lambda: ...)` — Durably record a pure synchronous computation.
- `await zart.now("ts")` — Deterministic, durably recorded UTC `datetime`.
- `zart.context()` — Returns `ExecutionInfo` (`execution_id`, `task_name`, `current_attempt`, `is_retry()`).

> **Note:** Zart's replay/suspension signal (`ZartControlFlowSignal`) inherits from `BaseException` rather than `Exception`, so `try ... except Exception:` in workflow code never accidentally swallows durable control flow.

## Typed Serialization

`zart.serde` (`to_json_value` / `from_json_value`) automatically serializes and reconstructs `dataclasses`, optional `pydantic.BaseModel`, `UUID`, `datetime`, `Enum`, `list[T]`, `dict[K, V]`, and `Optional[T]` / `T | None` across the FFI boundary.
