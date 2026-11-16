"""E-commerce checkout example using User-Managed SQLAlchemy AsyncSession + Zart.

Demonstrates:
- Declarative SQLAlchemy ORM models (Account, CheckoutOrder)
- User-managed transaction injection: ``session: AsyncSession = zart.trx(AsyncSessionLocal)``
- Direct ``await step(...)`` ergonomics (no ``zart.require(...)`` wrapper needed)
- Automatic SQLAlchemy ``rollback()`` on transient failure and ``flush()`` + ``commit()`` on retry
- End-to-end verification of final DB state via SQLAlchemy

Run against PostgreSQL:
    uv run python examples/sqlalchemy_checkout.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass

from sqlalchemy import Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import zart
from zart import DurableScheduler, PgBackend, Retry, WorkerBuilder

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://zart:zart@localhost:5432/zart"
)


def _sqlalchemy_url(url: str) -> str:
    if url.startswith("postgresql+"):
        return url
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "py_example_accounts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    balance: Mapped[int] = mapped_column(Integer, nullable=False)


class CheckoutOrder(Base):
    __tablename__ = "py_example_orders"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


@dataclass
class CheckoutRequest:
    order_id: str
    account_id: str
    sku: str
    amount: int


@dataclass
class CheckoutResult:
    order_id: str
    status: str
    remaining_balance: int


async def main() -> None:
    engine = create_async_engine(_sqlalchemy_url(DATABASE_URL), future=True)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    suffix = uuid.uuid4().hex[:8]
    account_id = f"acct-{suffix}"
    order_id = f"ord-{suffix}"

    # Seed initial account balance = 500
    async with AsyncSessionLocal() as session:
        session.add(Account(id=account_id, balance=500))
        await session.commit()

    charge_attempts = 0

    @zart.step("reserve-inventory")
    async def reserve_inventory(
        req: CheckoutRequest,
        session: AsyncSession = zart.trx(AsyncSessionLocal),
    ) -> str:
        session.add(
            CheckoutOrder(
                id=req.order_id,
                account_id=req.account_id,
                sku=req.sku,
                amount=req.amount,
                status="reserved",
            )
        )
        return req.order_id

    @zart.step("charge-account", retry=Retry.fixed(2, "50ms"))
    async def charge_account(
        req: CheckoutRequest,
        session: AsyncSession = zart.trx(AsyncSessionLocal),
    ) -> int:
        nonlocal charge_attempts
        charge_attempts += 1

        acct = await session.get(Account, req.account_id)
        assert acct is not None
        acct.balance -= req.amount

        # Simulate a transient failure on attempt 1: Zart rolls back the
        # SQLAlchemy session automatically so balance stays 500 before retry!
        if charge_attempts == 1:
            raise RuntimeError("transient payment gateway timeout")

        return acct.balance

    @zart.step("finalize-order")
    async def finalize_order(
        order_id: str,
        session: AsyncSession = zart.trx(AsyncSessionLocal),
    ) -> str:
        order = await session.get(CheckoutOrder, order_id)
        assert order is not None
        order.status = "paid"
        return order.status

    task_name = f"sqlalchemy-checkout-{suffix}"

    @zart.durable(task_name)
    async def checkout_workflow(req: CheckoutRequest) -> CheckoutResult:
        reserved_id = await reserve_inventory(req)
        remaining = await charge_account(req)
        final_status = await finalize_order(reserved_id)
        return CheckoutResult(
            order_id=reserved_id,
            status=final_status,
            remaining_balance=remaining,
        )

    backend = await PgBackend.connect(DATABASE_URL)
    await backend.run_migrations()
    scheduler = DurableScheduler(backend)
    worker = (
        WorkerBuilder(backend)
        .poll_interval(0.05)
        .register_durable(checkout_workflow)
        .build()
    )

    async def _worker_loop() -> None:
        await worker.run()

    worker_task = asyncio.create_task(_worker_loop())
    try:
        exec_id = f"checkout-{suffix}"
        req = CheckoutRequest(
            order_id=order_id,
            account_id=account_id,
            sku="zart-pro-plan",
            amount=150,
        )
        await scheduler.start(exec_id, task_name, zart.to_json_value(req))
        raw_out = await scheduler.wait_completion(
            exec_id, timeout_secs=10.0, poll_interval_secs=0.05
        )
        result = zart.from_json_value(raw_out, CheckoutResult)

        assert result.order_id == order_id
        assert result.status == "paid"
        assert result.remaining_balance == 350
        assert charge_attempts == 2

        # Verify final DB state independently via SQLAlchemy
        async with AsyncSessionLocal() as session:
            acct = (
                await session.execute(select(Account).where(Account.id == account_id))
            ).scalar_one()
            order = (
                await session.execute(
                    select(CheckoutOrder).where(CheckoutOrder.id == order_id)
                )
            ).scalar_one()
            assert acct.balance == 350
            assert order.status == "paid"

        print(
            f"Completed: order_id={result.order_id}, status={result.status}, "
            f"remaining_balance={result.remaining_balance}, attempts={charge_attempts}"
        )
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=10.0)
        await backend.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
