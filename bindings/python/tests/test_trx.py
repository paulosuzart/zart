"""Unit tests for zart.trx, parameter injection, and UserManagedTrxManager."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest
import zart
from zart import (
    ClientTrx,
    DurableScheduler,
    PgBackend,
    StepInvocation,
    Trx,
    WorkerBuilder,
    step,
    trx,
)
from zart.trx import (
    UserManagedTrxManager,
    get_worker_trx_provider,
    reset_worker_trx_provider,
    set_worker_trx_provider,
)


class _FakeSQLAlchemySession:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def flush(self) -> None:
        self.events.append("flush")

    async def commit(self) -> None:
        self.events.append("commit")

    async def rollback(self) -> None:
        self.events.append("rollback")

    async def close(self) -> None:
        self.events.append("close")


class _FakePsycopgConn:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def commit(self) -> None:
        self.events.append("commit")

    async def rollback(self) -> None:
        self.events.append("rollback")


class _FakePsycopgPool:
    def __init__(self, conn: _FakePsycopgConn) -> None:
        self._conn = conn
        self.exited_with: BaseException | None = None
        self.entered = False

    def connection(self) -> _FakePsycopgPool:
        return self

    async def __aenter__(self) -> _FakePsycopgConn:
        self.entered = True
        return self._conn

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        self.exited_with = exc
        return False


def test_step_omits_trx_parameter_from_caller() -> None:
    @step("debit-account")
    async def debit(account_id: str, amount: int, tx: Trx = trx()) -> str:
        return f"{account_id}:{amount}"

    inv = debit("acct-1", 50)
    assert inv.name == "debit-account"

    @step("credit-first-arg")
    async def credit(tx: Trx = trx(), account_id: str = "x") -> str:
        return account_id

    inv2 = credit("acct-2")
    assert inv2.name == "credit-first-arg"


def test_multiple_trx_params_raises() -> None:
    with pytest.raises(ValueError, match="at most one"):

        @step("bad-step")
        async def bad(a: Trx = trx(), b: Trx = trx()) -> None:
            pass


@pytest.mark.asyncio
async def test_sqlalchemy_session_lifecycle_success_and_error() -> None:
    session_ok = _FakeSQLAlchemySession()

    async with UserManagedTrxManager(lambda: session_ok) as s:
        assert s is session_ok
        s.events.append("query")

    assert session_ok.events == ["query", "flush", "commit", "close"]

    session_err = _FakeSQLAlchemySession()
    with pytest.raises(RuntimeError, match="boom"):
        async with UserManagedTrxManager(lambda: session_err) as s:
            s.events.append("query")
            raise RuntimeError("boom")

    assert session_err.events == ["query", "rollback", "close"]


@pytest.mark.asyncio
async def test_psycopg_connection_lifecycle_success_and_error() -> None:
    conn_ok = _FakePsycopgConn()
    pool_ok = _FakePsycopgPool(conn_ok)

    async with UserManagedTrxManager(pool_ok.connection) as conn:
        assert conn is conn_ok
        conn.events.append("execute")

    assert pool_ok.entered is True
    assert pool_ok.exited_with is None
    assert conn_ok.events == ["execute", "commit"]

    conn_err = _FakePsycopgConn()
    pool_err = _FakePsycopgPool(conn_err)
    with pytest.raises(ValueError, match="bad"):
        async with UserManagedTrxManager(pool_err.connection) as conn:
            conn.events.append("execute")
            raise ValueError("bad")

    assert isinstance(pool_err.exited_with, ValueError)
    assert conn_err.events == ["execute", "rollback"]


@pytest.mark.asyncio
async def test_async_generator_provider_lifecycle() -> None:
    events: list[str] = []

    async def get_db() -> AsyncGenerator[str]:
        events.append("open")
        try:
            yield "db-session"
            events.append("commit")
        except Exception:
            events.append("rollback")
            raise
        finally:
            events.append("close")

    async with UserManagedTrxManager(get_db) as db:
        assert db == "db-session"
        events.append("use")

    assert events == ["open", "use", "commit", "close"]

    events.clear()
    with pytest.raises(RuntimeError, match="fail"):
        async with UserManagedTrxManager(get_db) as db:
            assert db == "db-session"
            events.append("use")
            raise RuntimeError("fail")

    assert events == ["open", "use", "rollback", "close"]


@pytest.mark.asyncio
async def test_worker_default_provider_and_zart_trx_override() -> None:
    session = _FakeSQLAlchemySession()
    token = set_worker_trx_provider(lambda: session)
    try:

        @step("user-managed-default")
        async def with_default_session(x: int, db: Any = trx()) -> int:
            assert db is session
            return x + 1

        inv = with_default_session(41)
        assert isinstance(inv, StepInvocation)
        assert inv.name == "user-managed-default"

        async with UserManagedTrxManager(get_worker_trx_provider()) as s:
            assert s is session
            s.events.append("query")
        assert session.events == ["query", "flush", "commit", "close"]

        @step("zart-managed-override")
        async def with_zart_trx(x: int, tx: Trx = trx()) -> int:
            return x + 1

        inv_zart = with_zart_trx(10)
        assert isinstance(inv_zart, StepInvocation)
        assert inv_zart.name == "zart-managed-override"
    finally:
        reset_worker_trx_provider(token)


def test_phase3_exports_and_methods() -> None:
    assert Trx is zart.Trx
    assert ClientTrx is zart.ClientTrx
    assert hasattr(PgBackend, "begin")
    assert hasattr(DurableScheduler, "start_in_tx")
    assert hasattr(WorkerBuilder, "trx_provider")
