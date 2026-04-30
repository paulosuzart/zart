# Spec 0048 — Drop `zart_pause_snapshots` in Favour of Soft-Delete Audit

## Context / Problem

`zart_pause_snapshots` was introduced as a denormalized audit table capturing execution
state at the moment a pause rule became active. Its own schema comment reads:

> "Read-only history — not used for resume logic (zart_steps is authoritative)."

Despite the table existing in `0002_execution.sql`, **no Rust code ever references it**.
There are zero `SELECT`, `INSERT`, or query-builder references to `zart_pause_snapshots`
anywhere in the codebase.

At the same time, `zart_pause_rules` already carries full soft-delete semantics:

```sql
deleted_at  TIMESTAMPTZ,
deleted_by  TEXT
```

The active-rule partial index (`WHERE deleted_at IS NULL`) means soft-deleted rules are
already invisible to the scheduler without any extra table. The minimal audit need — *when
was a rule active and who deleted it* — is already answerable from `zart_pause_rules` alone.

The snapshot table adds schema surface area, migration complexity, and an undefined write
path with zero benefit.

## Alpha Note

Since the project is in **alpha**, existing migrations may be edited in-place rather than
adding a new migration file. The `zart_pause_snapshots` DDL will be removed directly from
`crates/zart/migrations/0002_execution.sql`. Anyone running a fresh database will never see
the table. Anyone with an existing dev database should reset it (`sqlx database reset`).
This approach is explicitly acceptable at alpha and avoids accumulating migration noise for
a table that was never written to.

## Goals

- Remove `zart_pause_snapshots` DDL from `0002_execution.sql` (in-place edit, alpha only).
- Add an optional `reason` column to `zart_pause_rules` to carry human-readable context
  when a rule is created or deleted (lightweight audit annotation).
- Thread `reason` through `PauseScope` → `PauseRule` → `PauseRequest` → `PauseRuleResponse`
  → `admin-demo` example.
- Ensure `ui-demo`, `admin-demo`, and `zart-api` compile cleanly with the updated structs.

## Non-Goals

- No full audit-log / event-sourcing system.
- No retroactive migration of snapshot data (table is empty — never written to).
- No changes to how `zart_steps` or `zart_execution_runs` record history.

## Design

### Part 1 — Remove `zart_pause_snapshots` from the migration

Edit `crates/zart/migrations/0002_execution.sql` directly:
- Delete the `CREATE TABLE IF NOT EXISTS zart_pause_snapshots` block and its index.

Add `reason TEXT` to `zart_pause_rules` in the same file:

```sql
CREATE TABLE IF NOT EXISTS zart_pause_rules (
    rule_id       TEXT PRIMARY KEY,
    execution_id  TEXT,
    task_name     TEXT,
    step_pattern  TEXT,
    reason        TEXT,          -- ← new: human-readable context for create/delete
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ,
    created_by    TEXT,
    deleted_at    TIMESTAMPTZ,
    deleted_by    TEXT
);
```

### Part 2 — Rust model changes

**`PauseScope`** (`crates/zart/src/admin/mod.rs`) gains:
```rust
/// Optional human-readable reason for this pause (audit annotation).
pub reason: Option<String>,
```

**`PauseRule`** (`crates/zart/src/admin/mod.rs`) gains:
```rust
pub reason: Option<String>,
```

**`PauseRequest`** (`crates/zart-api/src/models.rs`) gains:
```rust
#[serde(default)]
pub reason: Option<String>,
```

**`PauseRuleResponse`** (`crates/zart-api/src/models.rs`) gains:
```rust
#[serde(default)]
pub reason: Option<String>,
```

### Part 3 — Storage and service layer

- `crates/zart/src/postgres/` — UPDATE INSERT query for `zart_pause_rules` to write `reason`.
- SELECT queries that build `PauseRule` must fetch `reason`.
- `PauseService::pause()` passes `scope.reason` through to the INSERT.

### Part 4 — Examples and API wiring

**`admin-demo`** (`examples/admin-demo/src/main.rs`):
- Pass `reason: Some("demo-pause".to_string())` when calling `scheduler.pause(PauseScope { ... })`.
- Print the `reason` alongside `rule_id` in the pause section output.

**`ui-demo`** (`examples/ui-demo/`):
- No direct pause calls were found — verify it compiles without changes.

**`zart-api` admin routes** (`crates/zart-api/src/admin_routes.rs`):
- Map `req.reason` → `PauseScope.reason` in `create_pause`.
- Map `rule.reason` → `PauseRuleResponse.reason` in both `create_pause` and `list_pauses` responses.

### Audit Query (Before / After)

**Before** — answering "what pauses were active for execution X?" required a JOIN to
`zart_pause_snapshots` (whose write path was never implemented, so the answer was always
empty).

**After** — query `zart_pause_rules` directly:

```sql
SELECT rule_id, created_at, deleted_at, created_by, deleted_by, reason
FROM   zart_pause_rules
WHERE  execution_id = $1
ORDER  BY created_at DESC;
```

Active rules: add `AND deleted_at IS NULL`. Historical (soft-deleted): remove that filter.

## Files Affected

| File | Change |
|---|---|
| `crates/zart/migrations/0002_execution.sql` | Remove `zart_pause_snapshots` DDL + index; add `reason TEXT` to `zart_pause_rules` |
| `crates/zart/src/admin/mod.rs` | Add `reason: Option<String>` to `PauseScope` and `PauseRule` |
| `crates/zart/src/postgres/pause.rs` (or equivalent) | Update INSERT / SELECT to include `reason` |
| `crates/zart-api/src/models.rs` | Add `reason` to `PauseRequest` and `PauseRuleResponse` |
| `crates/zart-api/src/admin_routes.rs` | Wire `reason` through `create_pause` and `list_pauses` |
| `examples/admin-demo/src/main.rs` | Pass and print `reason` in pause section |
| Website / docs | Remove any mention of `zart_pause_snapshots`; document soft-delete audit pattern |

## Phase Plan

### Phase 1 — Schema edit (alpha in-place)
- Edit `0002_execution.sql`: remove snapshot DDL, add `reason TEXT`.
- Run `sqlx database reset` locally; verify migration applies cleanly.

### Phase 2 — Rust model + storage
- Add `reason` to `PauseScope`, `PauseRule` (admin/mod.rs).
- Update postgres INSERT/SELECT to write/read `reason`.
- Add `reason` to `PauseRequest`, `PauseRuleResponse` (zart-api/models.rs).
- Wire through `admin_routes.rs`.

### Phase 3 — Examples and tests
- Update `admin-demo` to pass and display `reason`.
- Confirm `ui-demo` compiles.
- Add unit test: create rule with reason, assert persisted and returned.
- Run all quality gates.

Phase 2 is blocked on Phase 1 (schema must land first for `sqlx prepare`).

## Rationale

- `zart_pause_rules` with `deleted_at` / `deleted_by` already encodes the full pause
  lifecycle. A separate snapshot table adds no new information.
- Editing the migration in-place is correct at alpha: no production data exists, and it
  avoids permanently accumulating a migration that creates and another that drops the same
  table within the same pre-v1 codebase.
- `reason` covers the one gap the snapshot table was presumably solving: "why was this
  pause applied?" — as a simple text field rather than a denormalized JSONB blob.

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| Developer with existing DB gets migration errors after reset | Document `sqlx database reset` in PR description; alpha norm |
| CI pipeline has a pre-existing DB state | CI should use ephemeral DBs; if not, `sqlx database reset` in CI setup step |
| `reason` semantics ambiguous (create-time vs delete-time) | Document in field doc comment: set on create; optionally overwritten at delete time by `deleted_by` companion; convention only |
| ui-demo has hidden pause path not found by grep | Compile-check in Phase 3 catches any missed reference |

## Breaking Changes

None for external consumers (alpha, no published stable API). Internally:

- `zart_pause_snapshots` is removed from the schema. Any tooling querying it will error.
  The table was never written to — impact is nil.
- `PauseScope` and `PauseRule` gain `reason: Option<String>`. Additive — existing struct
  literals using `..Default::default()` or named fields compile without changes. Any
  exhaustive destructuring will need the new field added.
- `PauseRequest` and `PauseRuleResponse` JSON schemas gain optional `reason` field.
  Existing clients that omit it will continue to work (`#[serde(default)]`).

## Definition of Done

- [ ] `just fmt` passes
- [ ] `just lint` passes
- [ ] All unit tests pass
- [ ] All integration tests pass (including example tests)
- [ ] `zart_pause_snapshots` does not appear in any non-`target/` SQL file
- [ ] `reason` column present in `zart_pause_rules`; readable and writable via storage layer
- [ ] `admin-demo` example passes and prints `reason`
- [ ] `ui-demo` compiles cleanly
- [ ] No module exceeds 600–700 lines (excluding tests)
- [ ] Website docs reviewed; `zart_pause_snapshots` references removed

## Notes

- Cross-reference: pause rules introduced alongside admin API (spec 0007 / 0027).
- If a richer audit trail is needed in the future, the correct approach is an append-only
  `zart_pause_events` table (insert-only, never updated) — not a denormalized snapshot.
