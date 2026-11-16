"""Order fulfillment & parallel saga example for Zart Python bindings.

Demonstrates:
1. ``@zart.durable`` and ``@zart.step`` with typed ``dataclass`` payloads.
2. Direct ``await my_step(...)`` without ``zart.require(...)``.
3. Durable loops with ``await process_line_item(...).named(f"item-{i}")``.
4. Parallel fan-out/fan-in via ``zart.schedule(...)`` + ``await zart.wait([...])``.
5. Graceful fallback via ``await zart.step_or(..., default)`` and ``Retry.fixed(...)``.
6. Durable ``await zart.sleep(...)``.

Run:
    uv run python examples/parallel_saga.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import UUID, uuid4

import zart


@dataclass
class LineItem:
    sku: str
    qty: int
    unit_price: float


@dataclass
class FulfillmentOrder:
    order_id: UUID
    customer_email: str
    customer_tier: str
    items: list[LineItem]


@dataclass
class FulfillmentResult:
    order_id: UUID
    subtotal: float
    discount: float
    total: float
    notifications: list[str]


@zart.step("validate-order")
async def validate_order(order: FulfillmentOrder) -> float:
    if not order.items:
        raise ValueError("Order must contain at least one item")
    return sum(item.qty * item.unit_price for item in order.items)


@zart.step("process-line-item", retry=zart.Retry.fixed(2, "20ms"))
async def process_line_item(item: LineItem) -> float:
    return item.qty * item.unit_price


@zart.step("fetch-promo-discount")
async def fetch_promo_discount(tier: str) -> float:
    if tier == "legacy-unmapped":
        raise RuntimeError("Promo service unavailable for legacy tier")
    return 15.0 if tier == "gold" else 0.0


@zart.step("notify-warehouse")
async def notify_warehouse(order_id: UUID, item_count: int) -> str:
    return f"warehouse-notified:{order_id}:{item_count}"


@zart.step("send-customer-receipt")
async def send_customer_receipt(email: str, total: float) -> str:
    return f"receipt-sent:{email}:{total:.2f}"


@zart.durable("fulfill-order")
async def fulfill_order(order: FulfillmentOrder) -> FulfillmentResult:
    await validate_order(order)

    subtotal = 0.0
    for i, item in enumerate(order.items):
        subtotal += await process_line_item(item).named(f"item-{i}")

    await zart.sleep("dispatch-cooldown", "20ms")

    h_warehouse = zart.schedule(notify_warehouse(order.order_id, len(order.items)))
    h_receipt = zart.schedule(send_customer_receipt(order.customer_email, subtotal))
    outcomes = await zart.wait([h_warehouse, h_receipt])
    notifications = [str(o.unwrap()) for o in outcomes]

    discount = await zart.step_or(fetch_promo_discount(order.customer_tier), 0.0)
    total = max(0.0, subtotal - discount)

    return FulfillmentResult(
        order_id=order.order_id,
        subtotal=subtotal,
        discount=discount,
        total=total,
        notifications=notifications,
    )


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL", "postgres://zart:zart@localhost:5432/zart")
    backend = await zart.PgBackend.connect(db_url)
    try:
        await backend.run_migrations()
        scheduler = zart.DurableScheduler(backend)

        order = FulfillmentOrder(
            order_id=uuid4(),
            customer_email="buyer@example.com",
            customer_tier="legacy-unmapped",
            items=[
                LineItem(sku="WIDGET-A", qty=2, unit_price=25.0),
                LineItem(sku="GADGET-B", qty=1, unit_price=50.0),
            ],
        )
        execution_id = f"saga-{order.order_id}"
        await scheduler.start(execution_id, "fulfill-order", zart.to_json_value(order))

        worker = zart.WorkerBuilder(backend).register_durable(fulfill_order).build()
        worker_task = asyncio.ensure_future(worker.run())
        try:
            raw = await scheduler.wait_completion(execution_id, timeout_secs=30.0)
            result = zart.from_json_value(raw, FulfillmentResult)
        finally:
            worker.stop()
            await worker_task

        assert result.subtotal == 100.0
        assert result.discount == 0.0
        assert result.total == 100.0
        assert len(result.notifications) == 2
        assert result.notifications[0].startswith("warehouse-notified:")
        assert result.notifications[1] == "receipt-sent:buyer@example.com:100.00"
        print(
            f"Completed saga {result.order_id}: "
            f"total={result.total}, notifications={result.notifications}"
        )
    finally:
        await backend.close()


if __name__ == "__main__":
    asyncio.run(main())
