"""Integration tests for PgBackend connectivity, ping, table names, and listener URL."""

from __future__ import annotations

import uuid

import pytest
import zart
from conftest import DATABASE_URL, run_with_worker
from zart import DurableScheduler, PgBackend, WorkerBuilder


@pytest.mark.asyncio
async def test_pg_backend_ping(backend: PgBackend) -> None:
    await backend.ping()


@pytest.mark.asyncio
async def test_pg_backend_connect_with_prefix_schema_and_listener(
    backend: PgBackend,
) -> None:
    del backend  # Ensures migrations ran via fixture before querying public.zart_* tables.
    custom_prefix_pg = await PgBackend.connect(DATABASE_URL, table_prefix="custom_")
    try:
        await custom_prefix_pg.ping()
    finally:
        await custom_prefix_pg.close()

    pg = await PgBackend.connect(
        DATABASE_URL,
        listener_url=DATABASE_URL,
        table_prefix="zart_",
        schema="public",
    )
    try:
        await pg.ping()
        pg_with_listener = pg.with_listener_url(DATABASE_URL)
        await pg_with_listener.ping()

        task_name = f"backend-cfg-{uuid.uuid4().hex[:10]}"
        exec_id = f"exec-cfg-{uuid.uuid4().hex[:10]}"

        @zart.durable(task_name)
        async def echo(value: str) -> str:
            return value

        scheduler = DurableScheduler(pg_with_listener)
        worker = (
            WorkerBuilder(pg_with_listener)
            .poll_interval(0.05)
            .heartbeat_interval(15.0)
            .register_durable(echo)
            .build()
        )
        await scheduler.start(exec_id, task_name, "ok")
        result = await run_with_worker(
            worker,
            scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05),
        )
        assert result == "ok"
    finally:
        await pg.close()

    with pytest.raises(RuntimeError, match="Ping failed"):
        await pg.ping()


@pytest.mark.asyncio
async def test_with_listener_url_preserves_custom_table_names(backend: PgBackend) -> None:
    del backend  # Ensures default zart_* tables exist so passing only works if names are preserved.
    custom_pg = await PgBackend.connect(DATABASE_URL, table_prefix="missing_")
    try:
        derived = custom_pg.with_listener_url(DATABASE_URL)
        scheduler = DurableScheduler(derived)
        with pytest.raises(RuntimeError, match="missing_executions"):
            await scheduler.start(
                f"exec-missing-{uuid.uuid4().hex[:8]}",
                "missing-task",
                {"hello": "world"},
            )
    finally:
        await custom_pg.close()
