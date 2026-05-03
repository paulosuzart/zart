# Spec 0052 — Zart Durable Objects

**Status: proposed (requires more thought especially on KV)**

## Context / Problem

Zart's `DurableExecution` trait models a workflow as a single `run()` function that
executes a pipeline of steps from start to finish. This covers batch and pipeline use
cases well but is a poor fit for **long-lived, interactive processes** where:

- The process exposes multiple named operations driven by external actors at
  unpredictable times (human approval, webhook callbacks, customer actions).
- Callers need to query the current phase of the process at any point.
- The "object" has identity — multiple concurrent instances, each owned by one entity
  (a user, an order, a document).

Examples: user onboarding, order lifecycle, document approval, subscription management.

Cloudflare Durable Objects offer a useful mental model: each object is a named actor
with colocated persistent storage, processed in a single thread. The CF DO runtime
enforces three correctness properties that Zart mirrors:

| CF DO concept | Zart equivalent |
|---|---|
| **Input gate** — new requests queue while a storage op is in progress | ULID message queue + pump task: messages queue while a handler is executing |
| **Intermediate state updates** — `put()` may be called multiple times mid-handler; coalesced atomically between await points | `ctx.transition()` and `ctx.set()` buffered in memory, flushed atomically at each step boundary |
| **Selective KV storage** — objects store many independent keys, loading only what is needed per operation | `ctx.get(key)` / `ctx.set(key, value)` / `ctx.delete(key)` backed by a unified `zart_object_storage` KV table |
| **State-gated dispatch** — only valid method invocations are accepted; others are rejected before the handler runs | `accepts(state, msg) -> bool` guard on the trait, evaluated before `handle()` |

---

## Goals

- Introduce a `ZartDurableObject` trait for defining a stateful actor with typed
  messages, explicit persistent state, and guarded transitions.
- Each actor instance is identified by a stable string ID (`object_id`).
- `send(msg)` enqueues the message in ULID order. A single pump task per instance
  processes messages one at a time — never concurrent, never out of order.
- Before dispatching, Zart calls `accepts(state, msg)`. If it returns `false`, the
  message is rejected (marked failed) without calling `handle()`.
- Inside `handle()`, the handler calls `ctx.transition(new_state)` to commit state
  updates. Multiple transitions between steps are coalesced — only the last one
  within a step interval is persisted atomically with the step result.
- For large or sparse data (e.g. a shopping cart's item list), handlers use
  `ctx.get(key)` / `ctx.set(key, value)` / `ctx.delete(key)` to load and write only
  the keys they need. All KV writes are buffered and flushed at step boundaries.
- `Self::State` is the **small header** — kept to phase, counters, and IDs. Large
  collections belong in KV keys, not in `Self::State`.
- `stub.state()` reads the persisted header state without scheduling any task.
- Steps inside `handle()` use the existing `ZartStep` machinery.
- Object instances map onto `zart_executions` (one row per instance) and
  `zart_execution_runs` (one row per message handled) for full admin visibility.

## Non-Goals

- Replacing `DurableExecution`; both patterns coexist.
- Compile-time typestate enforcement of transitions (runtime `accepts()` guard is sufficient).
- In-memory caching between invocations (Zart workers are stateless pollers).
- A proc-macro to auto-generate typed stub methods (follow-on).
- `wait_for_event` inside `handle()` — external signals arrive as messages.

---

## Design

### Trait

```rust
#[async_trait]
pub trait ZartDurableObject: Send + Sync + 'static {
    /// Persistent state carried between messages. `Default` seeds the initial value.
    type State: Serialize + DeserializeOwned + Default + Send + Clone;

    /// Message type — typically an enum of operations.
    type Message: Serialize + DeserializeOwned + Send;

    /// Guard evaluated before `handle()`. Return `false` to reject this message
    /// given the current state. Rejected messages are marked failed in the queue
    /// without calling `handle()`. Default: accept all.
    fn accepts(state: &Self::State, msg: &Self::Message) -> bool {
        true
    }

    /// Handle one incoming message. Use `ctx.transition()` to commit state updates.
    /// May be called multiple times if the task is retried; steps are idempotent.
    async fn handle(
        &self,
        ctx: &mut ObjectCtx<'_>,
        msg: Self::Message,
    ) -> Result<(), TaskError>;
}
```

**`Self::State` is the header — keep it small.** It is always loaded before every
message dispatch (for `accepts()`) and stored at the reserved KV key `"__state__"`.
It should hold only phase, counters, and small identifiers — not large collections.
Large or sparse data belongs in explicit KV keys accessed via `ctx.get()` / `ctx.set()`.

An enum is natural for phase-based objects; a struct works when several small fields
evolve independently:

```rust
// Enum state — phases are explicit; accepts() enforces valid transitions.
// Kept small: no large collections here.
#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(tag = "phase")]
enum OnboardState {
    #[default] Pending,
    IncomeValidated { name: String, score: u32 },
    AwaitingApproval { name: String, score: u32 },
    Approved,
    Rejected { reason: String },
}

// Struct state — small fields only. Large collections (e.g. items: Vec<CartItem>)
// must NOT be embedded here; use ctx.get("items") instead.
#[derive(Serialize, Deserialize, Default, Clone)]
struct CartHeader {
    item_count: u32,
    checked_out: bool,
}
```

---

### `ObjectCtx`

Wraps the existing execution context. Exposes step/sleep primitives, object identity,
the `transition()` / `state()` methods for the header state, and a CF DO-style
`get()` / `set()` / `delete()` API for selective KV storage.

```rust
pub struct ObjectCtx<'a> {
    inner: &'a mut ZartContext,
    // All pending writes (including "__state__") buffered here; flushed at step boundaries.
    pending_writes: HashMap<String, serde_json::Value>,
    pending_deletes: HashSet<String>,
    // In-memory view of current header state (loaded upfront for accepts()).
    current_state: serde_json::Value,
}

impl<'a> ObjectCtx<'a> {
    /// Execute a durable step. Flushes all pending KV writes atomically before
    /// recording the step result. Readers see the updated state as soon as the
    /// step completes.
    pub async fn step<S: ZartStep>(&mut self, step: S) -> Result<S::Output, TaskError>;

    pub async fn sleep(&mut self, label: &str, duration: Duration) -> Result<(), TaskError>;

    // ── Header state ─────────────────────────────────────────────────────────

    /// Read the current header state (reflects the latest transition in this handler).
    pub fn state<S: DeserializeOwned>(&self) -> Result<S, TaskError>;

    /// Buffer a header state transition. Sugar for `self.set("__state__", new_state)`.
    /// Coalesced with any previous unbuffered transition since the last step.
    /// Flushed atomically at the next `step()` or at handler completion.
    pub fn transition(&mut self, new_state: impl Serialize) -> Result<(), TaskError>;

    // ── Selective KV storage ─────────────────────────────────────────────────

    /// Load a single KV entry. Checks the pending write buffer first (no DB round
    /// trip if already written in this handler invocation).
    pub async fn get<V: DeserializeOwned>(&self, key: &str) -> Result<Option<V>, TaskError>;

    /// Buffer a KV write. Flushed atomically with all other pending writes at the
    /// next `step()` or at handler completion. Multiple `set()` calls for the same
    /// key between two steps are coalesced — only the last value is persisted.
    pub fn set<V: Serialize>(&mut self, key: &str, value: &V) -> Result<(), TaskError>;

    /// Buffer a KV delete. Coalesced with writes at the next flush.
    pub fn delete(&mut self, key: &str);

    // ── Identity ─────────────────────────────────────────────────────────────
    pub fn object_id(&self) -> &str;
    pub fn task_name(&self) -> &str;
}
```

**Coalescing rule:** `transition()`, `set()`, and `delete()` never touch the database
directly. At each `step()` boundary (and at handler completion), all pending writes and
deletes are flushed to `zart_object_storage` in a single batch upsert/delete, atomically
with the step result record. This mirrors CF DO's "writes coalesced between await
points" guarantee. `get()` checks the pending write buffer before hitting the DB, so
reads always see the latest in-handler value.

---

### Concurrency model — input gate via message queue

To give each object instance single-threaded semantics:

1. `send(msg)` inserts a row into `zart_object_messages` with a **ULID** primary key
   (time-ordered, safe under concurrent inserts from different processes). Returns
   immediately after the insert.
2. After inserting, `send()` idempotently ensures exactly one **pump task** is
   scheduled for the `(object_id, task_name)` pair in `zart_tasks`.
3. The pump task, protected by `SKIP LOCKED`, fetches the **oldest pending message**
   (`ORDER BY id ASC LIMIT 1`), evaluates `accepts()`, calls `handle()`, marks the
   message done, flushes any remaining pending state, then reschedules itself if
   further messages remain. Since only one pump runs per instance at a time, all
   messages are processed strictly in order with no concurrency — the input gate.

```sql
CREATE TABLE zart_object_messages (
    id          TEXT        NOT NULL,           -- ULID
    object_id   TEXT        NOT NULL,
    task_name   TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',  -- pending | processing | done | rejected
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX ON zart_object_messages (object_id, task_name, status, id);
```

---

### Execution / Run / Step hierarchy

Object instances map onto the existing hierarchy for full admin visibility:

| Existing concept | Durable Object mapping |
|---|---|
| `zart_executions` row | One row per object instance (`object_id`) — status stays `Running` |
| `zart_execution_runs` row | One row per message handled |
| `zart_steps` row | Steps executed inside a single message handler |

The pump task creates the execution record on first run and a new run record for each
message. This gives the admin API full visibility into every message ever processed and
its step history, using existing endpoints with no new API surface.

---

### KV storage (`zart_object_storage`)

Instead of a single blob per object instance, state is stored as a key-value table.
The reserved key `"__state__"` holds the serialized `Self::State` header. All other
keys are user-defined and loaded selectively.

```sql
CREATE TABLE zart_object_storage (
    object_id   TEXT        NOT NULL,
    task_name   TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_id, task_name, key)
);
```

The pump loads only `"__state__"` before calling `accepts()` and `handle()`. All
other keys are fetched lazily via `ctx.get(key)`. The pump owns the object's rows
exclusively during handler execution (single-threaded guarantee via queue); no
optimistic locking is needed.

Batch flush at step boundaries issues a single `INSERT ... ON CONFLICT DO UPDATE`
for all pending writes and a single `DELETE` for pending deletes.

---

### `ObjectStub<T>`

```rust
pub struct ObjectStub<T: ZartDurableObject> {
    object_id: String,
    task_name: String,
    scheduler: Arc<DurableScheduler>,
    _marker: PhantomData<T>,
}

impl<T: ZartDurableObject> ObjectStub<T> {
    /// Enqueue a message. Returns after inserting into the message queue.
    pub async fn send(&self, msg: &T::Message) -> Result<(), SchedulerError>;

    /// Read the current persisted state without enqueuing any task.
    pub async fn state(&self) -> Result<T::State, SchedulerError>;

    /// Poll until `predicate(state)` returns true or `timeout` elapses.
    pub async fn wait_state<F>(
        &self,
        predicate: F,
        timeout: Duration,
    ) -> Result<T::State, SchedulerError>
    where
        F: Fn(&T::State) -> bool + Send;
}
```

`DurableScheduler::object()` factory:

```rust
impl DurableScheduler {
    pub fn object<T: ZartDurableObject>(
        &self,
        task_name: &str,
        object_id: impl Into<String>,
    ) -> ObjectStub<T>;
}
```

### Registration

```rust
WorkerBuilder::from_backend(&pg)
    .register_durable_object("onboarding", OnboardingObject)
    .build();
```

The pump task is registered as `__zart_object__:onboarding`. This prefix cannot clash
with user-defined task names.

---

## Full Example — User Onboarding

Demonstrates: `accepts()` guard, intermediate `ctx.transition()` calls, invalid
transition rejection, and the two-message pattern for async external signals.

```rust
// ── State ────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
#[serde(tag = "phase")]
enum OnboardState {
    #[default]
    Pending,
    IncomeValidated { name: String, score: u32 },
    AwaitingApproval { name: String, score: u32 },
    Approved,
    Rejected { reason: String },
}

// ── Messages ─────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
enum OnboardMsg {
    Start { name: String, annual_income: f64 },
    Approve,
    Reject { reason: String },
}

// ── Object ───────────────────────────────────────────────────────────────────

pub struct OnboardingObject;

#[async_trait]
impl ZartDurableObject for OnboardingObject {
    type State = OnboardState;
    type Message = OnboardMsg;

    fn accepts(state: &OnboardState, msg: &OnboardMsg) -> bool {
        matches!(
            (state, msg),
            (OnboardState::Pending, OnboardMsg::Start { .. })
            | (OnboardState::AwaitingApproval { .. }, OnboardMsg::Approve)
            | (OnboardState::AwaitingApproval { .. }, OnboardMsg::Reject { .. })
        )
    }

    async fn handle(
        &self,
        ctx: &mut ObjectCtx<'_>,
        msg: OnboardMsg,
    ) -> Result<(), TaskError> {
        match msg {
            OnboardMsg::Start { name, annual_income } => {
                // Step 1: validate income
                let score = ctx.step(ValidateIncome { annual_income }).await?;

                // Intermediate transition — visible to readers after this step completes.
                // Coalesced with the step result atomically.
                ctx.transition(OnboardState::IncomeValidated {
                    name: name.clone(),
                    score,
                })?;

                // Step 2: notify reviewer (fire-and-forget)
                ctx.step(NotifyReviewer { name: name.clone(), score }).await?;

                // Final transition for this message
                ctx.transition(OnboardState::AwaitingApproval { name, score })?;
            }

            OnboardMsg::Approve => {
                let OnboardState::AwaitingApproval { name, .. } = ctx.state()? else {
                    unreachable!("accepts() guards this");
                };
                ctx.step(SendWelcomeEmail { name }).await?;
                ctx.transition(OnboardState::Approved)?;
            }

            OnboardMsg::Reject { reason } => {
                ctx.step(SendRejectionEmail { reason: reason.clone() }).await?;
                ctx.transition(OnboardState::Rejected { reason })?;
            }
        }
        Ok(())
    }
}

// ── Typed stub ───────────────────────────────────────────────────────────────

pub struct OnboardingHandle(ObjectStub<OnboardingObject>);

impl OnboardingHandle {
    pub fn new(scheduler: &DurableScheduler, user_id: Uuid) -> Self {
        Self(scheduler.object::<OnboardingObject>("onboarding", user_id.to_string()))
    }
    pub async fn start(&self, name: String, annual_income: f64) -> Result<()> {
        self.0.send(&OnboardMsg::Start { name, annual_income }).await
    }
    pub async fn approve(&self) -> Result<()> {
        self.0.send(&OnboardMsg::Approve).await
    }
    pub async fn reject(&self, reason: String) -> Result<()> {
        self.0.send(&OnboardMsg::Reject { reason }).await
    }
    pub async fn phase(&self) -> Result<OnboardState> {
        self.0.state().await
    }
}

// ── Application code ─────────────────────────────────────────────────────────

let handle = OnboardingHandle::new(&scheduler, user_id);

// User submits the onboarding form
handle.start("Alice".into(), 75_000.0).await?;

// Query state at any time — reflects latest committed transition
let phase = handle.phase().await?;
// Could be IncomeValidated or AwaitingApproval depending on worker progress

// Sending Approve while state is still Pending would be silently queued
// but rejected by accepts() when the pump picks it up

// Human reviewer approves (days later)
handle.approve().await?;

handle.wait_state(
    |s| matches!(s, OnboardState::Approved | OnboardState::Rejected { .. }),
    Duration::from_secs(10),
).await?;
```

---

## Example — Shopping Cart (Selective KV Loading)

Demonstrates `ctx.get()` / `ctx.set()` for large state. The header (`CartHeader`)
stays small; the item list lives at the `"items"` key and is only loaded when needed.

```rust
// ── Header state (always loaded — keep small) ─────────────────────────────────

#[derive(Serialize, Deserialize, Default, Clone)]
struct CartHeader {
    item_count: u32,
    checked_out: bool,
}

// ── Messages ──────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
enum CartMsg {
    AddItem    { sku: String, qty: u32 },
    RemoveItem { sku: String },
    Checkout,
}

#[derive(Serialize, Deserialize)]
struct CartItem { sku: String, qty: u32 }

// ── Object ────────────────────────────────────────────────────────────────────

pub struct ShoppingCart;

#[async_trait]
impl ZartDurableObject for ShoppingCart {
    type State = CartHeader;
    type Message = CartMsg;

    fn accepts(state: &CartHeader, msg: &CartMsg) -> bool {
        !state.checked_out  // no messages accepted once checked out
    }

    async fn handle(&self, ctx: &mut ObjectCtx<'_>, msg: CartMsg) -> Result<(), TaskError> {
        match msg {
            CartMsg::AddItem { sku, qty } => {
                // Load only the items list — not the whole object
                let mut items: Vec<CartItem> = ctx.get("items").await?.unwrap_or_default();
                items.push(CartItem { sku, qty });
                ctx.set("items", &items)?;
                // Update the small header counter (buffered alongside items write)
                let mut header: CartHeader = ctx.state()?;
                header.item_count = items.len() as u32;
                ctx.transition(header)?;
                // No step needed — pure state update, flushed at handler completion
            }

            CartMsg::RemoveItem { sku } => {
                let mut items: Vec<CartItem> = ctx.get("items").await?.unwrap_or_default();
                items.retain(|i| i.sku != sku);
                ctx.set("items", &items)?;
                let mut header: CartHeader = ctx.state()?;
                header.item_count = items.len() as u32;
                ctx.transition(header)?;
            }

            CartMsg::Checkout => {
                // Load items only for this operation
                let items: Vec<CartItem> = ctx.get("items").await?.unwrap_or_default();
                ctx.step(ProcessPayment { items }).await?;
                ctx.transition(CartHeader { item_count: 0, checked_out: true })?;
            }
        }
        Ok(())
    }
}

// ── Usage ─────────────────────────────────────────────────────────────────────

let cart = scheduler.object::<ShoppingCart>("cart", session_id.to_string());
cart.send(&CartMsg::AddItem { sku: "SKU-42".into(), qty: 2 }).await?;
cart.send(&CartMsg::AddItem { sku: "SKU-99".into(), qty: 1 }).await?;
cart.send(&CartMsg::Checkout).await?;

// Query just the header — never loads the item list
let header: CartHeader = cart.state().await?;
assert!(header.checked_out);
```

**Key difference from the onboarding example:** `AddItem` and `RemoveItem` make no
`step()` call — they are pure in-memory buffer updates flushed at handler completion.
The DB is written once per message, not once per field change.

---

## Schema Migrations

Two new migrations (applied in order):

```sql
-- 1. per-object message queue
CREATE TABLE zart_object_messages (
    id          TEXT        NOT NULL,           -- ULID
    object_id   TEXT        NOT NULL,
    task_name   TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',  -- pending | processing | done | rejected
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX ON zart_object_messages (object_id, task_name, status, id);

-- 2. per-object KV storage (replaces single-blob state; header at key "__state__")
CREATE TABLE zart_object_storage (
    object_id   TEXT        NOT NULL,
    task_name   TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_id, task_name, key)
);
```

No changes to existing tables. Existing `DurableExecution` workflows are unaffected.

---

## Files to Create / Modify

| File | Change |
|---|---|
| `crates/zart/src/durable_object.rs` | New: `ZartDurableObject`, `ObjectCtx` (KV buffer + coalescing), `ObjectStub`, pump task runner |
| `crates/zart/src/durable.rs` | Add `object()` factory |
| `crates/zart/src/builder.rs` | Add `register_durable_object()` |
| `crates/zart/src/lib.rs` | Re-export public types |
| `crates/zart-core/src/store/object_storage.rs` | New: `ObjectKvStorage` trait (`get_header`, `get_kv`, `flush_writes`, `dequeue_message`, `enqueue_message`, `ensure_pump_scheduled`) |
| `crates/zart/src/store/mod.rs` | Add `ObjectKvStorage` to `StorageBackend` blanket bound |
| `crates/zart/src/postgres/object_storage_impl.rs` | New: PG implementation of `ObjectKvStorage` |
| `migrations/` | Two new migrations (`zart_object_messages`, `zart_object_storage`) |
| `examples/durable-object/` | New example: onboarding actor (state machine) + shopping cart (KV selective loading) |

---

## Design Notes

### Why `transition()` and `set()` are sync, `step()` is async

`ctx.transition()` and `ctx.set()` only touch an in-memory buffer — never the
database. The flush happens atomically at the next `ctx.step()` call (before the
step result is recorded) or at handler completion. This mirrors CF DO's "coalesced
writes between await points": all synchronous operations between two awaits are
grouped into a single DB round trip.

A handler that has no steps at all (e.g. `CartMsg::AddItem`) flushes everything once
at handler completion — still a single write regardless of how many `set()` / `transition()`
calls were made.

### Why `Self::State` must stay small

`Self::State` is deserialized on every message dispatch, before `accepts()` is
evaluated. Embedding large collections (e.g. `Vec<CartItem>`) in `Self::State` would
load and deserialize them even for messages that never touch them (e.g. a header-only
status check). Large data belongs in explicit KV keys fetched via `ctx.get(key)` only
when the handler actually needs them.

### Why `wait_for_event` is excluded from `ObjectCtx`

An object waiting on an arbitrary event name would break the input gate guarantee:
the pump would be blocked indefinitely, preventing any further messages from being
processed. External signals must arrive as messages via `stub.send()`. If a handler
needs an async external signal mid-execution (e.g., a payment webhook arriving later),
the pattern is to split it into two messages (`PaymentInitiated`, `PaymentConfirmed`)
and hold the intermediate phase in state.

### Accepted messages that fail

If `handle()` returns `Err`, the message status is set to `failed` and the run record
is marked failed. Standard Zart retry semantics apply to the pump task — the same
message will be retried up to the configured limit. Steps already completed in a
previous attempt are not re-executed (idempotent step cache).
