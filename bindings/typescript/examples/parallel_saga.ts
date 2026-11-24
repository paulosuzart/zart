import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Data, Effect, Fiber, Schema } from "effect";
import {
	DurableScheduler,
	PgBackend,
	WorkerBuilder,
	Zart,
} from "../dist/index.js";

class OrderError extends Data.TaggedError("OrderError")<{
	readonly message: string;
}> {}
class PromoError extends Data.TaggedError("PromoError")<{
	readonly message: string;
}> {}

const LineItem = Schema.Struct({
	sku: Schema.String,
	qty: Schema.Number,
	unitPrice: Schema.Number,
});

const FulfillmentOrder = Schema.Struct({
	orderId: Zart.UuidString,
	customerEmail: Schema.String,
	customerTier: Schema.String,
	items: Schema.Array(LineItem),
});

const FulfillmentResult = Schema.Struct({
	orderId: Zart.UuidString,
	subtotal: Schema.Number,
	discount: Schema.Number,
	total: Schema.Number,
	notifications: Schema.Array(Schema.String),
});

const validateOrder = Zart.step(
	"validate-order",
	(order: typeof FulfillmentOrder.Type) =>
		order.items.length > 0
			? Effect.succeed(
					order.items.reduce((acc, item) => acc + item.qty * item.unitPrice, 0),
				)
			: Effect.fail(
					new OrderError({ message: "Order must contain at least one item" }),
				),
);

const processLineItem = Zart.step(
	"process-line-item",
	(item: typeof LineItem.Type) => Effect.succeed(item.qty * item.unitPrice),
	{ retry: Zart.Retry.fixed(2, "20 millis") },
);

const fetchPromoDiscount = Zart.step("fetch-promo-discount", (tier: string) =>
	tier === "legacy-unmapped"
		? Effect.fail(
				new PromoError({
					message: "Promo service unavailable for legacy tier",
				}),
			)
		: Effect.succeed(tier === "gold" ? 15.0 : 0.0),
);

const notifyWarehouse = Zart.step(
	"notify-warehouse",
	(orderId: string, itemCount: number) =>
		Effect.succeed(`warehouse-notified:${orderId}:${itemCount}`),
);

const sendCustomerReceipt = Zart.step(
	"send-customer-receipt",
	(email: string, total: number) =>
		Effect.succeed(`receipt-sent:${email}:${total.toFixed(2)}`),
);

const fulfillOrder = Zart.durable(
	"fulfill-order",
	(order) =>
		Effect.gen(function* () {
			yield* validateOrder(order);

			let subtotal = 0;
			for (const [i, item] of order.items.entries()) {
				subtotal += yield* processLineItem(item).named(`item-${i}`);
			}

			yield* Zart.sleep("dispatch-cooldown", "20 millis");

			const hWarehouse = yield* Zart.schedule(
				notifyWarehouse(order.orderId, order.items.length),
			);
			const hReceipt = yield* Zart.schedule(
				sendCustomerReceipt(order.customerEmail, subtotal),
			);
			const notifications = yield* Zart.waitAll([hWarehouse, hReceipt]);

			const discount = yield* Zart.stepOr(
				fetchPromoDiscount(order.customerTier),
				0.0,
			);
			const total = Math.max(0.0, subtotal - discount);

			return {
				orderId: order.orderId,
				subtotal,
				discount,
				total,
				notifications,
			};
		}),
	{ inputSchema: FulfillmentOrder, outputSchema: FulfillmentResult },
);

const main = Effect.scoped(
	Effect.gen(function* () {
		const dbUrl =
			process.env.DATABASE_URL ?? "postgres://zart:zart@localhost:5432/zart";
		const backend = yield* PgBackend.scoped(dbUrl);
		yield* backend.runMigrations();

		const scheduler = yield* DurableScheduler.fromBackend(backend);
		const orderId = randomUUID();
		const executionId = `saga-${orderId}`;

		yield* scheduler.start(executionId, "fulfill-order", FulfillmentOrder, {
			orderId,
			customerEmail: "buyer@example.com",
			customerTier: "legacy-unmapped",
			items: [
				{ sku: "WIDGET-A", qty: 2, unitPrice: 25.0 },
				{ sku: "GADGET-B", qty: 1, unitPrice: 50.0 },
			],
		});

		const worker = (yield* WorkerBuilder.fromBackend(backend))
			.registerDurable(fulfillOrder)
			.build();
		const workerFiber = yield* Effect.forkChild(worker.runScoped());
		const result = yield* scheduler
			.waitCompletion(executionId, FulfillmentResult, 30)
			.pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));

		assert.equal(result.subtotal, 100.0);
		assert.equal(result.discount, 0.0);
		assert.equal(result.total, 100.0);
		assert.equal(result.notifications.length, 2);
		assert.ok(result.notifications[0]?.startsWith("warehouse-notified:"));
		assert.equal(
			result.notifications[1],
			"receipt-sent:buyer@example.com:100.00",
		);
		console.log(
			`Completed saga ${result.orderId}: total=${result.total}, notifications=${JSON.stringify(result.notifications)}`,
		);
	}),
);

await Effect.runPromise(main);
