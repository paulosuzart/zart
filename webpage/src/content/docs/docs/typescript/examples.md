---
title: TypeScript Examples
description: Walkthroughs of the runnable TypeScript (Effect v4) examples in bindings/typescript/examples/.
---

The runnable examples live in `bindings/typescript/examples/` and run against PostgreSQL (`DATABASE_URL=postgres://zart:zart@localhost:5432/zart`).

From `bindings/typescript/`:

```bash
npm install
npm run build:native
npm run build
```

## 1. Transactional Scheduling & Atomic Steps (`transactions.ts`)

Demonstrates `backend.withTransaction((tx) => ...)` + `scheduler.startInTx(tx, ...)` to:
- Verify rollback atomicity when a transaction fails (neither the `demo_users` row nor the `zart_tasks` record persists)
- Atomically insert a `demo_users` row (`balance = 1000`) and schedule `onboard-user`
- Execute `Zart.step` functions using `Zart.Trx.fetchOne` and `Zart.Trx.execute` to deduct `100` (`final_balance = 900`) and set `onboarded = TRUE`

```bash
npm run example:transactions
```

## 2. Human-in-the-Loop Approval (`approval_workflow.ts`)

Demonstrates typed `Schema.Struct` payloads (`ApprovalRequest`, `ApprovalDecision`, `ApprovalOutput`), `Data.TaggedError` business errors (`ValidationError`), `yield* validateRequest(data)`, suspending on `yield* Zart.waitForEvent("manager-approval", { schema: ApprovalDecision, timeout: "2 minutes" })`, and resuming via `yield* scheduler.offerEvent(...)`.

```bash
npm run example:approval
```

## 3. Parallel Saga & Durable Loops (`parallel_saga.ts`)

Demonstrates a multi-step order fulfillment saga combining:
- Direct `yield* validateOrder(order)`
- Durable loop with `yield* processLineItem(item).named(\`item-${i}\`)` and `Zart.Retry.fixed(2, "20 millis")`
- Durable `yield* Zart.sleep("dispatch-cooldown", "20 millis")`
- Parallel fan-out/fan-in via `Zart.schedule(...)` + `yield* Zart.waitAll([...])`
- Graceful fallback via `yield* Zart.stepOr(fetchPromoDiscount(...), 0.0)`

```bash
npm run example:parallel-saga
```
