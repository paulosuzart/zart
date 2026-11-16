"""Unit tests for zart.serde and the FFI JSON serialization bridge."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, StrEnum
from typing import Optional
from uuid import UUID, uuid4

import pytest
from zart.serde import from_json_value, to_json_value


@dataclass
class Address:
    street: str
    zip_code: int
    unit: str | None = None


@dataclass
class UserOrder:
    order_id: UUID
    created_at: datetime
    amount: float
    paid: bool
    tags: list[str]
    address: Address
    items: list[Address] = field(default_factory=list)
    metadata: dict[str, int] = field(default_factory=dict)
    notes: str | None = None


def test_primitives_roundtrip() -> None:
    for val in [None, True, False, 0, 42, -7, 3.14, "hello", ""]:
        encoded = to_json_value(val)
        assert encoded == val
        assert from_json_value(encoded, type(val) if val is not None else None) == val


def test_uuid_and_datetime_roundtrip() -> None:
    uid = uuid4()
    now = datetime(2026, 11, 16, 12, 30, 45, tzinfo=UTC)

    enc_uid = to_json_value(uid)
    assert isinstance(enc_uid, str)
    assert from_json_value(enc_uid, UUID) == uid

    enc_dt = to_json_value(now)
    assert isinstance(enc_dt, str)
    assert from_json_value(enc_dt, datetime) == now


def test_nested_dataclass_roundtrip() -> None:
    order = UserOrder(
        order_id=uuid4(),
        created_at=datetime(2026, 11, 16, 8, 0, 0, tzinfo=UTC),
        amount=199.95,
        paid=True,
        tags=["priority", "fragile"],
        address=Address(street="123 Main St", zip_code=10001, unit="4B"),
        items=[
            Address(street="Warehouse A", zip_code=90210),
            Address(street="Depot B", zip_code=30301, unit=None),
        ],
        metadata={"retries": 2, "shard": 5},
        notes=None,
    )

    payload = to_json_value(order)
    assert isinstance(payload, dict)
    assert isinstance(payload["order_id"], str)
    assert isinstance(payload["created_at"], str)
    assert payload["address"] == {"street": "123 Main St", "zip_code": 10001, "unit": "4B"}

    restored = from_json_value(payload, UserOrder)
    assert restored == order
    assert isinstance(restored.order_id, UUID)
    assert isinstance(restored.created_at, datetime)
    assert isinstance(restored.address, Address)
    assert all(isinstance(item, Address) for item in restored.items)


def test_optional_and_collections() -> None:
    assert from_json_value(None, Optional[int]) is None  # noqa: UP045
    assert from_json_value(None, str | None) is None
    assert from_json_value(123, Optional[int]) == 123  # noqa: UP045

    uid = uuid4()
    raw = to_json_value({"k1": [uid], "k2": []})
    restored = from_json_value(raw, dict[str, list[UUID]])
    assert restored == {"k1": [uid], "k2": []}


class Status(StrEnum):
    PENDING = "pending"
    DONE = "done"


class Priority(int, Enum):
    LOW = 1
    HIGH = 10


@dataclass
class Job:
    status: Status
    priority: Priority


def test_enum_roundtrip() -> None:
    assert to_json_value(Status.PENDING) == "pending"
    assert from_json_value("pending", Status) is Status.PENDING
    assert to_json_value(Priority.HIGH) == 10
    assert from_json_value(10, Priority) is Priority.HIGH

    job = Job(status=Status.DONE, priority=Priority.LOW)
    encoded = to_json_value(job)
    assert encoded == {"status": "done", "priority": 1}
    assert from_json_value(encoded, Job) == job


def test_unsupported_type_raises() -> None:
    class Custom:
        pass

    with pytest.raises(TypeError, match="Unsupported type"):
        to_json_value(Custom())


def test_graceful_without_pydantic(monkeypatch: pytest.MonkeyPatch) -> None:
    import zart.serde as serde_mod

    monkeypatch.setattr(serde_mod, "_PydanticBaseModel", None)
    addr = Address(street="No Pydantic Ave", zip_code=12345)
    assert serde_mod.from_json_value(serde_mod.to_json_value(addr), Address) == addr


def test_pydantic_roundtrip() -> None:
    pydantic = pytest.importorskip("pydantic")

    class SubItem(pydantic.BaseModel):
        item_id: UUID
        label: str
        count: int
        note: str | None = None

    class Envelope(pydantic.BaseModel):
        request_id: UUID
        timestamp: datetime
        active: bool
        score: float
        items: list[SubItem]
        extra: str | None = None

    env = Envelope(
        request_id=uuid4(),
        timestamp=datetime(2026, 11, 16, 15, 45, 0, tzinfo=UTC),
        active=True,
        score=98.5,
        items=[
            SubItem(item_id=uuid4(), label="alpha", count=3, note="ok"),
            SubItem(item_id=uuid4(), label="beta", count=7),
        ],
        extra=None,
    )

    payload = to_json_value(env)
    assert isinstance(payload, dict)
    assert isinstance(payload["request_id"], str)
    assert isinstance(payload["timestamp"], str)

    restored = from_json_value(payload, Envelope)
    assert restored == env


def test_ffi_pythonize_roundtrip() -> None:
    _zart = pytest.importorskip("zart._zart")
    roundtrip = getattr(_zart, "_roundtrip_json", None)
    if roundtrip is None:
        pytest.skip("_roundtrip_json not exposed on zart._zart")

    order = UserOrder(
        order_id=uuid4(),
        created_at=datetime(2026, 11, 16, 10, 0, 0, tzinfo=UTC),
        amount=42.5,
        paid=False,
        tags=["a", "b"],
        address=Address(street="FFI Way", zip_code=77777),
        metadata={"k": 1},
        notes="via rust",
    )

    ffi_out = roundtrip(to_json_value(order))
    restored = from_json_value(ffi_out, UserOrder)
    assert restored == order
