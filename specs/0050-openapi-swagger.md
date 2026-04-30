# Spec 0050 — OpenAPI / Swagger for zart-api

## Context / Problem

`zart-api` exposes a rich HTTP surface — a Main API (`/api/v1/*`) and an Admin
API (`/admin/v1/*`) — but ships with no machine-readable API contract.
Consumers must hand-read source code to understand request/response shapes,
available parameters, and status codes.  Integrators who already use
[utoipa](https://github.com/juhaku/utoipa) in their own Axum applications
cannot incorporate Zart's schema into their existing `ApiDoc`.

This spec adds opt-in OpenAPI 3.x documentation via `utoipa`, covering all
endpoints in both the Main and Admin APIs, and optionally serving a Swagger UI
from the `ApiServer`.

---

## Goals

- Annotate every handler and model in `zart-api` with `utoipa` attributes when
  the `openapi` feature is enabled.
- Expose a `ZartApiDoc` type (implements `utoipa::OpenApi`) that users can
  **merge** into their own existing `ApiDoc`.
- Optionally mount `/openapi.json` + Swagger UI (via `utoipa-swagger-ui`) on
  `ApiServer` through an explicit builder call (`.with_swagger_ui()`).
- Zero overhead when the feature is disabled — no compile-time cost, no extra
  transitive dependencies.

## Non-Goals

- Authentication / authorisation on the Swagger UI endpoint.
- OpenAPI coverage for the Prometheus `/metrics` endpoint.
- Generating client SDKs from the schema.
- Supporting OpenAPI spec versions other than 3.x (utoipa default).

---

## Design / Proposal

### Part 1 — Cargo feature flag

Add an `openapi` feature to `crates/zart-api/Cargo.toml`:

```toml
[features]
metrics  = ["zart/metrics"]
openapi  = ["utoipa", "utoipa-swagger-ui"]

[dependencies]
utoipa             = { version = "5", features = ["axum_extras", "chrono", "uuid"], optional = true }
utoipa-swagger-ui  = { version = "9", features = ["axum"],                          optional = true }
```

All annotation code is wrapped in `#[cfg(feature = "openapi")]` blocks;
downstream crates that do not opt in see no change.

### Part 2 — Model annotations (`models.rs`)

Every request/response struct gains conditional `ToSchema` and `IntoParams`
derives:

```rust
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionResponse { … }

#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery { … }
```

All structs in `models.rs` are annotated this way:

| Struct | utoipa derive |
|---|---|
| `StartExecutionRequest` | `ToSchema` |
| `ListQuery` | `IntoParams` |
| `WaitQuery` | `IntoParams` |
| `ExecutionResponse` | `ToSchema` |
| `StartExecutionResponse` | `ToSchema` |
| `ErrorResponse` | `ToSchema` |
| `RetryStepRequest` | `ToSchema` |
| `RestartRequest` | `ToSchema` |
| `RerunRequest` | `ToSchema` |
| `RetryStepResponse` | `ToSchema` |
| `RestartResponse` | `ToSchema` |
| `RerunResponse` | `ToSchema` |
| `PotentiallyStaleDepResponse` | `ToSchema` |
| `RunRecordResponse` | `ToSchema` |
| `PauseRequest` | `ToSchema` |
| `PauseRuleResponse` | `ToSchema` |
| `ResumeResponse` | `ToSchema` |
| `StatsResponse` | `ToSchema` |
| `ExecutionDetailResponse` | `ToSchema` |
| `StepDetailResponse` | `ToSchema` |
| `StepAttemptResponse` | `ToSchema` |

### Part 3 — Handler annotations (`routes.rs`, `admin_routes.rs`)

Each handler gains a `#[cfg_attr(feature = "openapi", utoipa::path(…))]`
attribute listing method, path, params, request body, and responses.  Example:

```rust
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/executions",
    params(ListQuery),
    responses(
        (status = 200, description = "List of executions", body = Vec<ExecutionResponse>),
        (status = 500, description = "Internal error",     body = ErrorResponse),
    ),
    tag = "executions"
))]
async fn list_executions(…) { … }
```

Tags used:

| Tag | Handlers |
|---|---|
| `executions` | list, start, get, cancel, wait |
| `events` | deliver event |
| `stats` | stats |
| `health` | healthz, readyz |
| `admin-executions` | retry-step, restart, rerun, detail, runs |
| `admin-pause` | create pause rule, list pause rules, resume, delete rule |

### Part 4 — `ZartApiDoc` struct (`openapi.rs`)

A new module `crates/zart-api/src/openapi.rs`, compiled only under
`#[cfg(feature = "openapi")]`, exports:

```rust
/// Merges Zart's API schema into a user's existing utoipa ApiDoc.
///
/// # Example
///
/// ```rust
/// #[derive(utoipa::OpenApi)]
/// #[openapi(
///     nest(
///         (path = "/", api = ZartApiDoc)
///     )
/// )]
/// struct MyAppDoc;
/// ```
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        routes::list_executions,
        routes::start_execution,
        routes::get_execution,
        routes::cancel_execution,
        routes::wait_execution,
        routes::deliver_event,
        routes::stats,
        routes::healthz,
        routes::readyz,
        admin_routes::retry_step,
        admin_routes::restart_execution,
        admin_routes::rerun_execution,
        admin_routes::get_detail,
        admin_routes::list_runs,
        admin_routes::create_pause_rule,
        admin_routes::list_pause_rules,
        admin_routes::resume_pause_rule,
        admin_routes::delete_pause_rule,
    ),
    components(schemas(
        StartExecutionRequest, ListQuery, WaitQuery, ExecutionResponse,
        StartExecutionResponse, ErrorResponse,
        RetryStepRequest, RestartRequest, RerunRequest,
        RetryStepResponse, RestartResponse, RerunResponse,
        PotentiallyStaleDepResponse, RunRecordResponse,
        PauseRequest, PauseRuleResponse, ResumeResponse,
        StatsResponse, ExecutionDetailResponse, StepDetailResponse, StepAttemptResponse,
    )),
    tags(
        (name = "executions",       description = "Durable execution lifecycle"),
        (name = "events",           description = "External event delivery"),
        (name = "stats",            description = "Aggregate execution statistics"),
        (name = "health",           description = "Liveness and readiness probes"),
        (name = "admin-executions", description = "Administrative execution operations"),
        (name = "admin-pause",      description = "Pause rule management"),
    ),
    info(title = "Zart API", version = env!("CARGO_PKG_VERSION"))
)]
pub struct ZartApiDoc;
```

This type is **re-exported** from `zart_api::openapi::ZartApiDoc` so users can
`use zart_api::openapi::ZartApiDoc` in their own crate.

### Part 5 — `ApiServer::with_swagger_ui()` (`server.rs`)

The builder gains an optional method (feature-gated) that mounts
`/openapi.json` and the Swagger UI under a configurable prefix (default:
`/swagger-ui`):

```rust
impl ApiServer {
    /// Mount Swagger UI at `/swagger-ui` and serve the schema at `/openapi.json`.
    /// Only available with the `openapi` feature.
    #[cfg(feature = "openapi")]
    pub fn with_swagger_ui(mut self) -> Self {
        self.swagger_ui = true;
        self
    }
}
```

When `swagger_ui` is `true`, `router()` merges additional routes:

```rust
#[cfg(feature = "openapi")]
if self.swagger_ui {
    use utoipa_swagger_ui::SwaggerUi;
    app = app.merge(
        SwaggerUi::new("/swagger-ui")
            .url("/openapi.json", ZartApiDoc::openapi()),
    );
}
```

No authentication is added (Non-goal).  The Swagger UI is accessible to any
client that can reach the server.

---

## Before / After Scenarios

### Before

A user wanting API docs must read `routes.rs` or guess from the JS/REST client.
There is no way to integrate Zart's API schema into an existing utoipa setup.

### After — integration with an existing utoipa setup

```rust
// In user's binary:
use zart_api::openapi::ZartApiDoc;

#[derive(utoipa::OpenApi)]
#[openapi(nest((path = "/", api = ZartApiDoc)))]
struct MyAppDoc;

// serve /openapi.json from MyAppDoc as usual
```

### After — built-in Swagger UI

```rust
ApiServer::new(addr, durable)
    .with_swagger_ui()   // only available with `openapi` feature
    .serve()
    .await?;
// Swagger UI now at http://addr/swagger-ui
// Schema JSON at   http://addr/openapi.json
```

### After — feature disabled (default)

```toml
# user's Cargo.toml — no `openapi` feature enabled
zart-api = { version = "…" }
```

Zero overhead. No utoipa transitive deps pulled in.

---

## Files Affected

| File | Change |
|---|---|
| `crates/zart-api/Cargo.toml` | Add `openapi` feature; add `utoipa` + `utoipa-swagger-ui` optional deps |
| `crates/zart-api/src/models.rs` | Add `#[cfg_attr(feature = "openapi", derive(ToSchema/IntoParams))]` to all types |
| `crates/zart-api/src/routes.rs` | Add `#[cfg_attr(feature = "openapi", utoipa::path(…))]` to all handlers |
| `crates/zart-api/src/admin_routes.rs` | Same as above for admin handlers |
| `crates/zart-api/src/openapi.rs` | **New file** — `ZartApiDoc` struct + re-exports |
| `crates/zart-api/src/server.rs` | Add `swagger_ui: bool` field, `with_swagger_ui()` builder method, conditional router merge |
| `crates/zart-api/src/lib.rs` | Declare `openapi` module; re-export `ZartApiDoc` behind feature flag |

---

## Phase Plan

### Phase 1 — Dependencies & feature flag
- Add `openapi` feature to `Cargo.toml` with optional deps.
- Confirm `just fmt` + `just lint` pass with no code changes yet.

### Phase 2 — Model annotations
- Add `ToSchema` / `IntoParams` derives to all structs in `models.rs`.
- Verify compilation with `--features openapi` and without.

### Phase 3 — Handler annotations
- Add `utoipa::path` attributes to all handlers in `routes.rs`.
- Add `utoipa::path` attributes to all handlers in `admin_routes.rs`.

### Phase 4 — `ZartApiDoc` + `openapi.rs`
- Create `openapi.rs` with the `ZartApiDoc` struct listing all paths and schemas.
- Re-export from `lib.rs`.
- Verify `ZartApiDoc::openapi()` serialises to valid JSON (`serde_json::to_string`).

### Phase 5 — `ApiServer::with_swagger_ui()`
- Add `swagger_ui` field to `ApiServer`.
- Implement `with_swagger_ui()` builder method.
- Wire Swagger UI into `router()` behind feature gate.

### Phase 6 — Tests & quality gates
- Add a unit test that calls `ZartApiDoc::openapi()` and asserts non-empty paths list.
- Add a unit test that `ApiServer::with_swagger_ui()` is present and compiles.
- Run `just fmt`, `just lint`, full integration + example tests.
- Review website docs for API reference pages.

> Phase 2 must precede Phase 3 (handlers reference schema types).
> Phase 3 must precede Phase 4 (ApiDoc lists all handler paths).
> Phase 4 must precede Phase 5 (server uses `ZartApiDoc`).

---

## Rationale

**Why utoipa?**  It is the de-facto standard for OpenAPI generation in Axum
ecosystems, supports schema merging (`nest`), and has first-class `axum_extras`
support that matches our handler signatures.

**Why a `ZartApiDoc` re-export rather than only a built-in endpoint?**
Many production deployments already have an Axum app with their own utoipa
setup. Forcing users to hit a separate `/openapi.json` path is invasive; the
`nest` merge pattern is the idiomatic utoipa answer.

**Why `with_swagger_ui()` instead of always-on?**  Swagger UI adds ~500 KB of
bundled assets to the binary.  Opt-in keeps the default footprint small and
avoids confusion about unauthenticated endpoints in production.

**Why no auth on Swagger UI?**  First iteration; auth strategies vary widely
(basic auth, OAuth2, API keys).  Users can add their own Axum middleware in
front of the path if needed.

---

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| utoipa version pinning conflicts with user's own utoipa dep | Document minimum compatible versions; use workspace resolver = "2" |
| `utoipa-swagger-ui` binary size bloat | Feature-gated; disabled by default |
| Annotation maintenance burden as handlers change | Integration test on `ZartApiDoc::openapi()` will fail if a path is removed without updating the doc |
| `IntoParams` on `ListQuery` may miss custom deserialization nuances | Document enum-valued string params explicitly in `#[param]` attributes |
| Admin API uses concrete `Arc<DurableScheduler>` state — utoipa doesn't care, but path registration must be explicit | All admin paths listed manually in `ZartApiDoc::paths(…)` |

---

## Breaking Changes

None. The `openapi` feature is additive. Existing users who do not opt in see
no API, binary, or compile-time change.

---

## Definition of Done

- [ ] `just fmt` passes
- [ ] `just lint` passes
- [ ] All unit tests pass (including new `ZartApiDoc` serialization test)
- [ ] All integration tests pass
- [ ] All example tests pass
- [ ] `cargo build --features openapi` succeeds for `zart-api`
- [ ] `cargo build` (no feature) succeeds without pulling in utoipa
- [ ] `ZartApiDoc` is re-exported from `zart_api::openapi`
- [ ] `ApiServer::with_swagger_ui()` mounts `/swagger-ui` and `/openapi.json`
- [ ] No module in `zart-api` exceeds 600–700 lines (excl. tests)
- [ ] Website API reference reviewed for needed updates

---

## Notes

- utoipa 5.x is required for the `nest` merge pattern; older 4.x used `merge` directly.
- `utoipa-swagger-ui` version must be kept in sync with `utoipa`; check the
  utoipa release matrix when bumping.
- Future work: add Bearer token security scheme to `ZartApiDoc` so Swagger UI
  can attach an `Authorization` header.
