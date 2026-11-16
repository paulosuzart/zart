"""Shared pytest fixtures for Zart Python integration tests."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable
from typing import Any

import pytest_asyncio
from psycopg_pool import AsyncConnectionPool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from zart import DurableScheduler, PgBackend, Worker

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


@pytest_asyncio.fixture
async def backend() -> AsyncIterator[PgBackend]:
    pg = await PgBackend.connect(DATABASE_URL)
    await pg.run_migrations()
    try:
        yield pg
    finally:
        await pg.close()


@pytest_asyncio.fixture
async def scheduler(backend: PgBackend) -> DurableScheduler:
    return DurableScheduler(backend)


@pytest_asyncio.fixture
async def sa_session_maker() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(_sqlalchemy_url(DATABASE_URL), future=True)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def psycopg_pool() -> AsyncIterator[AsyncConnectionPool[Any]]:
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=4,
        open=False,
    )
    await pool.open()
    try:
        yield pool
    finally:
        await pool.close()


async def run_with_worker(worker: Worker, coro: Awaitable[Any]) -> Any:
    """Run ``worker.run()`` in the background while awaiting ``coro``, then stop cleanly."""
    async def _worker_loop() -> None:
        await worker.run()

    worker_task = asyncio.create_task(_worker_loop())
    try:
        return await coro
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=10.0)
