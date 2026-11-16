"""Transaction participation example for Zart Python bindings.

Demonstrates:
1. ``start_in_tx``: atomically creating a user row and scheduling a durable
   execution in the same database transaction.
2. ``zart.trx()``: deducting account balance atomically with step completion.

Run:
    uv run python examples/transactions.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import UUID, uuid4

import zart


@dataclass
class OnboardInput:
    user_id: UUID
    email: str


@dataclass
class OnboardOutput:
    user_id: UUID
    email: str
    balance: int


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


@zart.step("mark-onboarded")
async def mark_onboarded(user_id: UUID, tx: zart.Trx = zart.trx()) -> None:
    await tx.execute("UPDATE demo_users SET onboarded = TRUE WHERE id = $1", user_id)


@zart.durable("onboard-user")
async def onboard_user(data: OnboardInput) -> OnboardOutput:
    balance = await zart.require(deduct_balance(data.user_id, 100))
    await zart.require(mark_onboarded(data.user_id))
    return OnboardOutput(user_id=data.user_id, email=data.email, balance=balance)


async def ensure_schema(backend: zart.PgBackend) -> None:
    async with await backend.begin() as tx:
        await tx.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_users (
                id        UUID PRIMARY KEY,
                email     TEXT NOT NULL,
                balance   BIGINT NOT NULL DEFAULT 0,
                onboarded BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL", "postgres://zart:zart@localhost:5432/zart")
    backend = await zart.PgBackend.connect(db_url)
    try:
        await backend.run_migrations()
        await ensure_schema(backend)

        scheduler = zart.DurableScheduler(backend)
        user_id = uuid4()
        email = f"user-{user_id}@example.com"
        execution_id = f"onboard-{user_id}"

        async with await backend.begin() as tx:
            await tx.execute(
                "INSERT INTO demo_users (id, email, balance) VALUES ($1, $2, 1000)",
                user_id,
                email,
            )
            await scheduler.start_in_tx(
                tx,
                execution_id,
                "onboard-user",
                zart.to_json_value(OnboardInput(user_id=user_id, email=email)),
            )

        print(f"Created user {email} (balance=1000) and scheduled {execution_id} atomically")

        worker = zart.WorkerBuilder(backend).register_durable(onboard_user).build()
        worker_task = asyncio.ensure_future(worker.run())
        try:
            raw_result = await scheduler.wait_completion(execution_id, timeout_secs=30.0)
            output = zart.from_json_value(raw_result, OnboardOutput)
        finally:
            worker.stop()
            await worker_task

        assert output.balance == 900, f"Expected balance 900, got {output.balance}"

        async with await backend.begin() as tx:
            row = await tx.fetch_one(
                "SELECT balance, onboarded FROM demo_users WHERE id = $1",
                user_id,
            )
        assert row["balance"] == 900
        assert row["onboarded"] is True

        print(f"Completed: user={output.email}, final_balance={output.balance}")
    finally:
        await backend.close()


if __name__ == "__main__":
    asyncio.run(main())
