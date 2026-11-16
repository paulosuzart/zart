"""Integration tests for transactional step completion and start_in_tx."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import zart
from zart import DurableScheduler, PgBackend, WorkerBuilder

from conftest import run_with_worker


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest_asyncio.fixture
async def accounts_table(backend: PgBackend) -> None:
    async with await backend.begin() as tx:
        await tx.execute(
            """
            CREATE TABLE IF NOT EXISTS py_test_accounts (
                id TEXT PRIMARY KEY,
                balance INT NOT NULL
            )
            """
        )


@pytest.mark.asyncio
async def test_start_in_tx_commit_and_rollback(
    backend: PgBackend, scheduler: DurableScheduler, accounts_table: None
) -> None:
    acct_ok = _uid("acct-ok")
    exec_ok = _uid("exec-tx-ok")
    task_name = _uid("tx-noop")

    @zart.durable(task_name)
    async def noop(val: str) -> str:
        return val

    # 1. Atomic commit of business row + execution schedule
    async with await backend.begin() as tx:
        await tx.execute(
            "INSERT INTO py_test_accounts (id, balance) VALUES ($1, $2)",
            acct_ok,
            1000,
        )
        await scheduler.start_in_tx(tx, exec_ok, task_name, acct_ok)

    async with await backend.begin() as tx:
        row = await tx.fetch_one(
            "SELECT balance FROM py_test_accounts WHERE id = $1", acct_ok
        )
    assert row["balance"] == 1000

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(noop).build()
    res = await run_with_worker(
        worker,
        scheduler.wait_completion(exec_ok, timeout_secs=10.0, poll_interval_secs=0.05),
    )
    assert res == acct_ok

    # 2. Rollback leaves neither account row nor execution record
    acct_rb = _uid("acct-rb")
    exec_rb = _uid("exec-tx-rb")
    with pytest.raises(RuntimeError, match="abort"):
        async with await backend.begin() as tx:
            await tx.execute(
                "INSERT INTO py_test_accounts (id, balance) VALUES ($1, $2)",
                acct_rb,
                500,
            )
            await scheduler.start_in_tx(tx, exec_rb, task_name, acct_rb)
            raise RuntimeError("abort")

    async with await backend.begin() as tx:
        row_rb = await tx.fetch_optional(
            "SELECT balance FROM py_test_accounts WHERE id = $1", acct_rb
        )
    assert row_rb is None
    with pytest.raises(LookupError):
        await scheduler.status(exec_rb)


@pytest.mark.asyncio
async def test_zart_managed_trx_commit_and_rollback(
    backend: PgBackend, scheduler: DurableScheduler, accounts_table: None
) -> None:
    acct_id = _uid("acct-zart")
    async with await backend.begin() as tx:
        await tx.execute(
            "INSERT INTO py_test_accounts (id, balance) VALUES ($1, $2)",
            acct_id,
            1000,
        )

    @zart.step("debit-zart")
    async def debit(amount: int, account: str, tx: zart.Trx = zart.trx()) -> int:
        row = await tx.fetch_one(
            "UPDATE py_test_accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
            amount,
            account,
        )
        return int(row["balance"])

    @zart.step("debit-zart-fail")
    async def debit_fail(amount: int, account: str, tx: zart.Trx = zart.trx()) -> int:
        await tx.execute(
            "UPDATE py_test_accounts SET balance = balance - $1 WHERE id = $2",
            amount,
            account,
        )
        raise RuntimeError("simulated step failure")

    task_ok = _uid("zart-trx-ok")
    task_fail = _uid("zart-trx-fail")

    @zart.durable(task_ok)
    async def wf_ok(account: str) -> int:
        return await debit(100, account)

    @zart.durable(task_fail)
    async def wf_fail(account: str) -> int:
        return await debit_fail(250, account)

    worker = (
        WorkerBuilder(backend)
        .poll_interval(0.05)
        .register_durable(wf_ok)
        .register_durable(wf_fail)
        .build()
    )

    exec_ok = _uid("exec-ztrx-ok")
    exec_fail = _uid("exec-ztrx-fail")

    async def _run() -> tuple[int, Any]:
        await scheduler.start(exec_ok, task_ok, acct_id)
        bal = await scheduler.wait_completion(
            exec_ok, timeout_secs=10.0, poll_interval_secs=0.05
        )
        await scheduler.start(exec_fail, task_fail, acct_id)
        rec_fail = await scheduler.wait(
            exec_fail, timeout_secs=10.0, poll_interval_secs=0.05
        )
        return bal, rec_fail

    bal, rec_fail = await run_with_worker(worker, _run())
    assert bal == 900
    assert rec_fail.status == "failed"

    async with await backend.begin() as tx:
        row = await tx.fetch_one(
            "SELECT balance FROM py_test_accounts WHERE id = $1", acct_id
        )
        step_row = await tx.fetch_one(
            "SELECT status::text AS status FROM zart_steps WHERE run_id LIKE $1 AND step_name = 'debit-zart'",
            f"{exec_ok}:%",
        )
    assert row["balance"] == 900
    assert step_row["status"] == "completed"


@pytest.mark.asyncio
async def test_user_managed_sqlalchemy_commit_and_rollback(
    backend: PgBackend,
    scheduler: DurableScheduler,
    sa_session_maker: async_sessionmaker[AsyncSession],
    accounts_table: None,
) -> None:
    acct_id = _uid("acct-sa")
    async with await backend.begin() as tx:
        await tx.execute(
            "INSERT INTO py_test_accounts (id, balance) VALUES ($1, $2)",
            acct_id,
            1000,
        )

    @zart.step("sa-debit")
    async def sa_debit(
        amount: int,
        account: str,
        session: AsyncSession = zart.trx(sa_session_maker),
    ) -> int:
        res = await session.execute(
            text(
                "UPDATE py_test_accounts SET balance = balance - :amt "
                "WHERE id = :acct RETURNING balance"
            ),
            {"amt": amount, "acct": account},
        )
        return int(res.scalar_one())

    @zart.step("sa-debit-fail")
    async def sa_debit_fail(
        amount: int,
        account: str,
        session: AsyncSession = zart.trx(sa_session_maker),
    ) -> int:
        await session.execute(
            text("UPDATE py_test_accounts SET balance = balance - :amt WHERE id = :acct"),
            {"amt": amount, "acct": account},
        )
        raise RuntimeError("sqlalchemy rollback")

    task_ok = _uid("sa-ok")
    task_fail = _uid("sa-fail")

    @zart.durable(task_ok)
    async def wf_ok(account: str) -> int:
        return await sa_debit(150, account)

    @zart.durable(task_fail)
    async def wf_fail(account: str) -> int:
        return await sa_debit_fail(300, account)

    worker = (
        WorkerBuilder(backend)
        .poll_interval(0.05)
        .register_durable(wf_ok)
        .register_durable(wf_fail)
        .build()
    )

    exec_ok = _uid("exec-sa-ok")
    exec_fail = _uid("exec-sa-fail")

    async def _run() -> tuple[int, str]:
        await scheduler.start(exec_ok, task_ok, acct_id)
        bal = await scheduler.wait_completion(
            exec_ok, timeout_secs=10.0, poll_interval_secs=0.05
        )
        await scheduler.start(exec_fail, task_fail, acct_id)
        rec = await scheduler.wait(
            exec_fail, timeout_secs=10.0, poll_interval_secs=0.05
        )
        return bal, rec.status

    bal, fail_status = await run_with_worker(worker, _run())
    assert bal == 850
    assert fail_status == "failed"

    async with await backend.begin() as tx:
        row = await tx.fetch_one(
            "SELECT balance FROM py_test_accounts WHERE id = $1", acct_id
        )
    assert row["balance"] == 850


@pytest.mark.asyncio
async def test_user_managed_psycopg_commit_and_rollback(
    backend: PgBackend,
    scheduler: DurableScheduler,
    psycopg_pool: AsyncConnectionPool[Any],
    accounts_table: None,
) -> None:
    acct_id = _uid("acct-psy")
    async with await backend.begin() as tx:
        await tx.execute(
            "INSERT INTO py_test_accounts (id, balance) VALUES ($1, $2)",
            acct_id,
            1000,
        )

    @zart.step("psy-debit")
    async def psy_debit(
        amount: int,
        account: str,
        conn: Any = zart.trx(psycopg_pool.connection),
    ) -> int:
        cur = await conn.execute(
            "UPDATE py_test_accounts SET balance = balance - %s WHERE id = %s RETURNING balance",
            (amount, account),
        )
        row = await cur.fetchone()
        assert row is not None
        return int(row[0])

    @zart.step("psy-debit-fail")
    async def psy_debit_fail(
        amount: int,
        account: str,
        conn: Any = zart.trx(psycopg_pool.connection),
    ) -> int:
        await conn.execute(
            "UPDATE py_test_accounts SET balance = balance - %s WHERE id = %s",
            (amount, account),
        )
        raise RuntimeError("psycopg rollback")

    task_ok = _uid("psy-ok")
    task_fail = _uid("psy-fail")

    @zart.durable(task_ok)
    async def wf_ok(account: str) -> int:
        return await zart.require(psy_debit(200, account))

    @zart.durable(task_fail)
    async def wf_fail(account: str) -> int:
        return await zart.require(psy_debit_fail(400, account))

    worker = (
        WorkerBuilder(backend)
        .poll_interval(0.05)
        .register_durable(wf_ok)
        .register_durable(wf_fail)
        .build()
    )

    exec_ok = _uid("exec-psy-ok")
    exec_fail = _uid("exec-psy-fail")

    async def _run() -> tuple[int, str]:
        await scheduler.start(exec_ok, task_ok, acct_id)
        bal = await scheduler.wait_completion(
            exec_ok, timeout_secs=10.0, poll_interval_secs=0.05
        )
        await scheduler.start(exec_fail, task_fail, acct_id)
        rec = await scheduler.wait(
            exec_fail, timeout_secs=10.0, poll_interval_secs=0.05
        )
        return bal, rec.status

    bal, fail_status = await run_with_worker(worker, _run())
    assert bal == 800
    assert fail_status == "failed"

    async with await backend.begin() as tx:
        row = await tx.fetch_one(
            "SELECT balance FROM py_test_accounts WHERE id = $1", acct_id
        )
    assert row["balance"] == 800
