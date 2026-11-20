"""Transaction injection markers, ContextVar state, and user-managed transaction lifecycles."""

from __future__ import annotations

import contextvars
import inspect
from collections.abc import AsyncGenerator
from typing import Any

from zart._zart import ClientTrx, Trx, _get_worker_trx_provider

_worker_trx_provider: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "zart_worker_trx_provider", default=None
)


class _TrxMarker:
    """Sentinel default value returned by :func:`trx` for parameter injection."""

    def __init__(self, provider: Any = None) -> None:
        self.provider = provider

    def __repr__(self) -> str:
        return f"_TrxMarker(provider={self.provider!r})"


def trx(provider: Any = None) -> Any:
    """Declare a transaction/session parameter to be injected into a ``@step`` function.

    - ``tx: zart.Trx = zart.trx()`` — Zart-managed ``Trx`` (or worker default provider
      when the parameter is not annotated as ``zart.Trx``).
    - ``session: AsyncSession = zart.trx(AsyncSessionLocal)`` — User-managed provider.
    """
    return _TrxMarker(provider)


def get_worker_trx_provider() -> Any:
    """Return the active worker-level transaction provider, if any."""
    native = _get_worker_trx_provider()
    if native is not None:
        return native
    return _worker_trx_provider.get()


def set_worker_trx_provider(provider: Any) -> contextvars.Token[Any]:
    """Set the worker-level transaction provider in the current context."""
    return _worker_trx_provider.set(provider)


def reset_worker_trx_provider(token: contextvars.Token[Any]) -> None:
    """Reset the worker-level transaction provider using a ContextVar token."""
    _worker_trx_provider.reset(token)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class UserManagedTrxManager:
    """Async context manager managing user-provided DB sessions/connections.

    Supports:
    - SQLAlchemy ``async_sessionmaker`` / ``AsyncSession`` (``flush`` + ``commit`` on
      success, ``rollback`` on exception, ``close`` in ``finally``)
    - ``psycopg_pool.AsyncConnectionPool.connection`` / ``AsyncConnection``
      (``async with provider() as conn:``, ``commit`` on success, ``rollback`` on error)
    - FastAPI-style async generator dependencies (``async def get_db(): yield session``)
    - Generic async context managers or callables
    """

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self._agen: AsyncGenerator[Any] | None = None
        self._cm: Any = None
        self._resource: Any = None

    async def __aenter__(self) -> Any:
        provider = self._provider
        if inspect.isasyncgenfunction(provider):
            self._agen = provider()
            self._resource = await self._agen.__anext__()
            return self._resource

        obj = provider() if callable(provider) else provider
        if inspect.isawaitable(obj):
            obj = await obj

        if hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__"):
            self._cm = obj
            self._resource = await obj.__aenter__()
            return self._resource

        self._resource = obj
        return self._resource

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        if self._agen is not None:
            return await self._exit_async_gen(exc)

        res = self._resource
        try:
            if res is not None:
                await self._finalize_resource(res, exc_type)
        finally:
            if self._cm is not None:
                await self._cm.__aexit__(exc_type, exc, tb)
            elif res is not None and hasattr(res, "close"):
                await _maybe_await(res.close())
        return False

    async def _exit_async_gen(self, exc: BaseException | None) -> bool:
        agen = self._agen
        assert agen is not None
        try:
            if exc is None:
                try:
                    await agen.__anext__()
                except StopAsyncIteration:
                    pass
            else:
                try:
                    await agen.athrow(exc)
                except StopAsyncIteration:
                    pass
        finally:
            await agen.aclose()
        return False

    async def _finalize_resource(
        self,
        res: Any,
        exc_type: type[BaseException] | None,
    ) -> None:
        has_commit = hasattr(res, "commit")
        has_rollback = hasattr(res, "rollback")
        has_flush = hasattr(res, "flush")

        if exc_type is None:
            if has_flush:
                await _maybe_await(res.flush())
            if has_commit:
                await _maybe_await(res.commit())
        else:
            if has_rollback:
                await _maybe_await(res.rollback())


__all__ = [
    "ClientTrx",
    "Trx",
    "UserManagedTrxManager",
    "_TrxMarker",
    "_worker_trx_provider",
    "get_worker_trx_provider",
    "reset_worker_trx_provider",
    "set_worker_trx_provider",
    "trx",
]
