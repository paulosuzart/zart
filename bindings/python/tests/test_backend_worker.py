"""Unit tests for PgBackend validation/configuration and WorkerBuilder options."""

from __future__ import annotations

import pytest
from zart import PgBackend, Worker, WorkerBuilder


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"table_prefix": "bad-prefix"}, "invalid characters"),
        ({"table_prefix": ""}, "cannot be empty"),
        ({"table_prefix": "1bad"}, "must start with a letter"),
        ({"table_prefix": "a" * 64}, "identifier limit"),
        ({"schema": "1bad"}, "must start with a letter"),
        ({"schema": "bad-schema"}, "invalid characters"),
        ({"schema": ""}, "cannot be empty"),
        ({"table_prefix": "valid_", "schema": "1bad"}, "must start with a letter"),
    ],
)
async def test_pg_backend_connect_rejects_invalid_identifiers(
    kwargs: dict[str, str], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        await PgBackend.connect("postgres://zart:zart@localhost:5432/zart", **kwargs)


@pytest.mark.asyncio
async def test_pg_backend_connect_rejects_invalid_listener_url() -> None:
    with pytest.raises(RuntimeError, match="Invalid listener URL"):
        await PgBackend.connect(
            "postgres://zart:zart@localhost:5432/zart",
            listener_url="not a valid url",
        )


def test_pg_backend_lazy_validates_identifiers_and_listener_url() -> None:
    with pytest.raises(ValueError, match="invalid characters"):
        PgBackend._connect_lazy(
            "postgres://zart:zart@localhost:5432/zart",
            table_prefix="bad-prefix",
        )
    with pytest.raises(ValueError, match="must start with a letter"):
        PgBackend._connect_lazy(
            "postgres://zart:zart@localhost:5432/zart",
            schema="1bad",
        )
    with pytest.raises(ValueError, match="Invalid PostgreSQL URL"):
        PgBackend._connect_lazy("not a postgres url")
    with pytest.raises(RuntimeError, match="Invalid listener URL"):
        PgBackend._connect_lazy(
            "postgres://zart:zart@localhost:5432/zart",
            listener_url="not a valid url",
        )

    pg = PgBackend._connect_lazy(
        "postgres://zart:zart@localhost:5432/zart",
        listener_url="postgres://zart:zart@pg-direct:5432/zart",
        table_prefix="svc_",
        schema="tenant_a",
    )
    assert isinstance(pg, PgBackend)

    with_listener = pg.with_listener_url("postgres://zart:zart@pg-other:5432/zart")
    assert isinstance(with_listener, PgBackend)
    assert with_listener is not pg

    with pytest.raises(RuntimeError, match="Invalid listener URL"):
        pg.with_listener_url("not a valid url")


@pytest.mark.asyncio
async def test_pg_backend_ping_raises_runtime_error_when_closed() -> None:
    pg = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    await pg.close()
    with pytest.raises(RuntimeError, match="Ping failed"):
        await pg.ping()


def test_worker_builder_heartbeat_interval_and_validation() -> None:
    pg = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    builder = WorkerBuilder(pg)

    assert builder.heartbeat_interval(None) is builder
    assert isinstance(builder.build(), Worker)
    assert builder.heartbeat_interval(0.0) is builder
    assert isinstance(builder.build(), Worker)
    assert builder.heartbeat_interval(15.0) is builder
    assert builder.heartbeat_interval(None) is builder

    for bad_secs in (-1.0, float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="non-negative finite"):
            builder.heartbeat_interval(bad_secs)

    with pytest.raises(ValueError, match="out of range"):
        builder.heartbeat_interval(1e30)

    with pytest.raises(ValueError, match="max_concurrent_tasks must be at least 1"):
        builder.max_concurrent_tasks(0)

    with pytest.raises(ValueError, match="max_tasks_per_poll must be at least 1"):
        builder.max_tasks_per_poll(0)

    for fn in (builder.poll_interval, builder.shutdown_timeout, builder.orphan_timeout):
        with pytest.raises(ValueError, match="non-negative finite"):
            fn(-1.0)
        with pytest.raises(ValueError, match="non-negative finite"):
            fn(float("nan"))

    worker = (
        builder.poll_interval(0.5)
        .max_concurrent_tasks(4)
        .max_tasks_per_poll(2)
        .shutdown_timeout(5.0)
        .orphan_timeout(60.0)
        .heartbeat_interval(15.0)
        .enable_listen_notify(False)
        .build()
    )
    assert isinstance(worker, Worker)
