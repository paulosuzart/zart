# Spec 0051 — Configurable Route Prefixes

**Status: proposed**

## Context / Problem

Spec 0050 introduced the `openapi` feature for `zart-api`, hardcoding two route
prefixes throughout the codebase:

| Prefix | Location |
|---|---|
| `/api/v1` | `routes::api_router()` — each `.route()` call and every `utoipa::path` macro |
| `/zart/admin/v1` | `admin_routes::admin_router()` — each `.route()` call and every `utoipa::path` macro |

Users embedding `zart-api` inside their own Axum application often have an
existing path convention (e.g., `/v2`, `/internal/zart`, `/ops`). Today they must
either patch the source or accept a clash. Worse, if they use the `openapi`
feature, the generated OpenAPI spec will still list the hardcoded prefixes even
if they somehow reroute the axum router, because the path strings in
`utoipa::path` macros are compile-time constants.

`utoipa-axum`'s `OpenApiRouter` solves this cleanly: handler paths in
`utoipa::path` macros become **relative** (e.g., `/executions`), and the prefix
is applied once at nest time via `OpenApiRouter::nest(&prefix, sub_router)`. Both
the HTTP routes and the generated OpenAPI spec reflect the runtime prefix
automatically.

## Goals

- Allow users to set the main API prefix (default `"/api/v1"`) and the admin
  prefix (default `"/zart/admin/v1"`) without recompiling.
- Swagger UI and `/openapi.json` must reflect the actual runtime prefixes.
- Defaults remain backward-compatible with spec 0050.
- `ZartApiDoc` remains exportable for users building their own router outside
  `ApiServer`.
- Prefix configuration is available via environment variables
  (`ZART_API_PREFIX`, `ZART_ADMIN_PREFIX`) as a convenience, with builder
  methods taking precedence.

## Non-Goals

- Per-handler path customisation.
- Changing prefixes after server startup (static at startup time is fine).
- Changing health check paths (`/healthz`, `/readyz`) or metrics path
  (`/metrics`) — these stay at root and are unaffected by the prefix.

## Design

### Part 1 — Relative handler paths in routes.rs and admin_routes.rs

Strip the prefix from every `utoipa::path` macro and every `.route()` call.
Paths become relative to whatever prefix is applied at nest time.

**routes.rs** — current vs. proposed handler path strings:

| Handler | Current | Proposed |
|---|---|---|
| `list_executions` / `start_execution` | `/api/v1/executions` | `/executions` |
| `get_execution` | `/api/v1/executions/{execution_id}` | `/executions/{execution_id}` |
| `cancel_execution` | `/api/v1/executions/{execution_id}/cancel` | `/executions/{execution_id}/cancel` |
| `wait_execution` | `/api/v1/executions/{execution_id}/wait` | `/executions/{execution_id}/wait` |
| `offer_event` | `/api/v1/events/{execution_id}/{event_name}` | `/events/{execution_id}/{event_name}` |
| `get_stats` | `/api/v1/stats` | `/stats` |
| `healthz` | `/healthz` | unchanged (root-level) |
| `readyz` | `/readyz` | unchanged (root-level) |

Health checks and metrics are mounted **outside** the nested prefix because they
serve infrastructure concerns at the root.

**admin_routes.rs** — same strip: `/zart/admin/v1/executions/...` → `/executions/...`, etc.

`api_router(state: AppState)` gains a `prefix: &str` parameter so the routes are
nested correctly regardless of whether the `openapi` feature is active.
`admin_router()` gains the same parameter.

### Part 2 — utoipa-axum under the `openapi` feature

Add `utoipa-axum` as an optional dependency under the `openapi` feature:

```toml
# Cargo.toml (zart-api)
utoipa-axum = { version = "0.1", optional = true }

[features]
openapi = ["utoipa", "utoipa-swagger-ui", "utoipa-axum"]
```

Under `#[cfg(feature = "openapi")]`, `api_router()` and `admin_router()` return
`utoipa_axum::router::OpenApiRouter` instead of `axum::Router`. The `OpenApiRouter`
carries collected path metadata; callers call `.split_for_parts()` to separate the
axum router from the accumulated OpenAPI spec fragment.

When the `openapi` feature is **not** enabled, the signatures remain `axum::Router`
unchanged — the prefix is applied via plain `axum::Router::nest()`.

### Part 3 — ApiServer prefix fields and builder methods

Add `api_prefix` and `admin_prefix` fields to `ApiServer` (always present, not
feature-gated, so prefix configuration works even without `openapi`):

```rust
pub struct ApiServer {
    addr: String,
    durable: Arc<dyn DurableApi>,
    cancellation: Option<CancellationToken>,
    api_prefix: String,
    admin_prefix: String,
    #[cfg(feature = "openapi")]
    swagger_ui: bool,
}
```

Builder methods:

```rust
/// Override the main API route prefix (default: "/api/v1").
/// Builder call takes precedence over the ZART_API_PREFIX env var.
#[must_use]
pub fn with_api_prefix(mut self, prefix: impl Into<String>) -> Self {
    self.api_prefix = prefix.into();
    self
}

/// Override the admin API route prefix (default: "/zart/admin/v1").
/// Builder call takes precedence over the ZART_ADMIN_PREFIX env var.
#[must_use]
pub fn with_admin_prefix(mut self, prefix: impl Into<String>) -> Self {
    self.admin_prefix = prefix.into();
    self
}
```

`ApiServer::new()` resolves defaults at construction time (builder call overrides):

```rust
fn default_api_prefix() -> String {
    std::env::var("ZART_API_PREFIX").unwrap_or_else(|_| "/api/v1".to_string())
}

fn default_admin_prefix() -> String {
    std::env::var("ZART_ADMIN_PREFIX").unwrap_or_else(|_| "/zart/admin/v1".to_string())
}
```

Precedence: **builder method > env var > compiled default**.

### Part 4 — router() assembly with runtime prefixes

**Without `openapi` feature** — plain axum nesting:

```rust
pub fn router(&self) -> Router {
    let state = AppState::new(self.durable.clone());
    let api   = routes::api_router(state.clone(), &self.api_prefix);
    let admin = admin_routes::admin_router(state, &self.admin_prefix);

    Router::new()
        .merge(api)
        .merge(admin)
        .route("/healthz", get(routes::healthz_handler))
        .route("/readyz",  get(routes::readyz_handler))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
```

**With `openapi` feature** — `OpenApiRouter::nest` so the spec reflects the
runtime prefix:

```rust
#[cfg(feature = "openapi")]
pub fn router(&self) -> Router {
    use utoipa_axum::router::OpenApiRouter;

    let state = AppState::new(self.durable.clone());
    let api_oar   = routes::api_router(state.clone(), &self.api_prefix);
    let admin_oar = admin_routes::admin_router(state, &self.admin_prefix);

    let combined = OpenApiRouter::new()
        .merge(api_oar)
        .merge(admin_oar);

    let (axum_router, openapi) = combined.split_for_parts();

    let mut app = Router::new()
        .merge(axum_router)
        .route("/healthz", get(routes::healthz_handler))
        .route("/readyz",  get(routes::readyz_handler));

    if self.swagger_ui {
        use utoipa_swagger_ui::SwaggerUi;
        app = app.merge(
            SwaggerUi::new("/swagger-ui").url("/openapi.json", openapi)
        );
    }

    app.layer(TraceLayer::new_for_http())
       .layer(CorsLayer::permissive())
}
```

Each sub-router applies its own prefix internally via `OpenApiRouter::nest()`,
so `combined.split_for_parts()` yields an OpenAPI spec whose paths already
contain the runtime prefixes.

### Part 5 — ZartApiDoc compatibility

`ZartApiDoc` (the static `#[derive(utoipa::OpenApi)]` struct) is used by library
consumers who build their own router and merge the Zart spec into their own
`ApiDoc` via the `nest` attribute. This use-case is preserved:

- `ZartApiDoc` keeps its `#[derive(utoipa::OpenApi)]` form with **relative** paths
  (no prefix in path strings). Consumers control the prefix via their own `nest`.
- `openapi.rs` gains a new free function
  `build_openapi(api_prefix: &str, admin_prefix: &str) -> utoipa::openapi::OpenApi`
  for callers who want a fully prefixed spec value without standing up an
  `ApiServer`.
- `swagger_ui_router()` is updated to accept optional prefix strings (or removed
  in favour of using `ApiServer` directly).

## Before / After Scenarios

### Scenario A — Default usage (backward compatible)

**Before (spec 0050):**
```rust
ApiServer::new("0.0.0.0:8080", durable)
    .with_swagger_ui()
    .serve().await?;
// Routes: /api/v1/executions, /zart/admin/v1/executions, …
// OpenAPI: hardcoded /api/v1/executions
```

**After:**
```rust
ApiServer::new("0.0.0.0:8080", durable)
    .with_swagger_ui()
    .serve().await?;
// Routes: /api/v1/executions, /zart/admin/v1/executions (unchanged)
// OpenAPI: same — defaults preserved
```

### Scenario B — Custom prefixes via builder

```rust
ApiServer::new("0.0.0.0:8080", durable)
    .with_api_prefix("/v2")
    .with_admin_prefix("/ops/zart")
    .with_swagger_ui()
    .serve().await?;
// Routes: /v2/executions, /ops/zart/executions, …
// OpenAPI /openapi.json: paths are /v2/executions, /ops/zart/executions
```

### Scenario C — Env-var driven configuration

```bash
ZART_API_PREFIX=/internal/api ZART_ADMIN_PREFIX=/internal/admin ./my-app
```
```rust
// No builder call needed — env vars picked up in ApiServer::new()
ApiServer::new("0.0.0.0:8080", durable).with_swagger_ui().serve().await?;
// Routes: /internal/api/executions, /internal/admin/executions
```

### Scenario D — Library consumer with custom ApiDoc

```rust
use zart_api::openapi::ZartApiDoc;

#[derive(utoipa::OpenApi)]
#[openapi(nest(
    (path = "/v2", api = ZartApiDoc)
))]
struct MyAppDoc;
// ZartApiDoc paths are relative; nesting at /v2 yields /v2/executions, etc.
```

## Files Affected

| File | Change |
|---|---|
| `crates/zart-api/Cargo.toml` | Add `utoipa-axum` optional dep under `openapi` feature |
| `crates/zart-api/src/routes.rs` | Strip `/api/v1` from all `.route()` calls and `utoipa::path` macros; accept `prefix` param; feature-gate return type |
| `crates/zart-api/src/admin_routes.rs` | Strip `/zart/admin/v1` from all `.route()` calls and `utoipa::path` macros; accept `prefix` param; feature-gate return type |
| `crates/zart-api/src/server.rs` | Add `api_prefix`, `admin_prefix` fields; add builder methods; rework `router()` with feature-gated nesting logic |
| `crates/zart-api/src/openapi.rs` | Update `ZartApiDoc` path annotations to relative form; add `build_openapi()` helper |
| `crates/zart-api/src/lib.rs` | Re-export `build_openapi` if made public |
| `examples/` | No change — examples use `ApiServer::new()` with defaults |
| Website docs | Update `zart-api` usage page to document prefix configuration and env vars |

## Phase Plan

### Phase 1 — Strip prefixes from handler macros and route registrations

- In `routes.rs`: change every `utoipa::path(path = "/api/v1/...")` to the
  relative form and every `.route("/api/v1/...")` to `.route("/...")`.
- In `admin_routes.rs`: same for `/zart/admin/v1/...`.
- Both `api_router()` and `admin_router()` accept a `prefix: &str` parameter and
  use `Router::nest(prefix, inner)` internally.
- Health/metrics routes stay at root, extracted as standalone handler fns.
- Update unit tests: requests previously sent to `/api/v1/executions` now go
  through a `test_app()` that nests at `/api/v1`.
- **Gate**: `cargo test` passes.

### Phase 2 — Add utoipa-axum; feature-gate OpenApiRouter return types

- Add `utoipa-axum` to `Cargo.toml` under `openapi` feature.
- Under `#[cfg(feature = "openapi")]`, change `api_router()` and `admin_router()`
  to return `OpenApiRouter`; use `cfg_attr` / a feature-gated wrapper to keep the
  non-openapi path returning `axum::Router`.
- Update `openapi.rs` `ZartApiDoc` path annotations to relative strings.
- **Gate**: `cargo test` (no features) and `cargo test --features openapi` both pass.

### Phase 3 — ApiServer prefix fields and builder methods

- Add `api_prefix`, `admin_prefix` to `ApiServer`; resolve env vars in `new()`.
- Add `with_api_prefix()` and `with_admin_prefix()` builder methods.
- Pass prefix values into `api_router()` / `admin_router()`.
- **Gate**: existing `server_builds_router` and `with_swagger_ui_builds_router`
  tests pass unmodified; new tests for custom prefix and env-var resolution pass.

### Phase 4 — Runtime prefix nesting and OpenAPI spec assembly

- Rework `ApiServer::router()` as described in Part 4.
- Add `build_openapi()` helper in `openapi.rs`.
- Add a test asserting that with `with_api_prefix("/v2")` the generated OpenAPI
  JSON contains `/v2/executions` as a path key.
- **Gate**: all quality gates pass (fmt, lint, unit, integration, example tests).

## Rationale

**Why `utoipa-axum` only under the `openapi` feature?**
Without `openapi`, plain `axum::Router::nest()` already applies prefixes to
routes correctly. `utoipa-axum` is only needed when the OpenAPI spec must
reflect the prefix dynamically.

**Why keep `ZartApiDoc` in addition to dynamic spec generation?**
Library consumers who embed `zart-api` into their own Axum app and maintain their
own `ApiDoc` derive need a mergeable type, not a runtime `OpenApi` value. Both
patterns are valid.

**Why builder methods AND env vars?**
Builder methods are idiomatic Rust and discoverable via docs. Env vars are useful
in containerised deployments where the binary is not recompiled per environment.
Builder calls always win so tests can override env vars without `std::env::set_var`
races.

**Why keep health/readyz/metrics outside the prefix?**
Infrastructure probes follow cluster conventions that differ from API versioning.
Coupling them to the API prefix would force users who want `/v2/executions` to
accept `/v2/healthz`, breaking existing probe configuration.

**Why pass `prefix` as a parameter to `api_router()` / `admin_router()` rather
than applying it in `ApiServer::router()` only?**
Each sub-router needs the prefix at construction time so `OpenApiRouter::nest()`
can embed it into the accumulated OpenAPI fragment before `split_for_parts()` is
called. Post-hoc rewriting of OpenAPI path keys is fragile.

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| `cfg`-gated return types cause duplicated logic across feature boundaries | Extract all handler logic into functions independent of router type; feature gate only the router construction wrapper |
| `utoipa-axum` API surface changes between minor versions | Pin to an exact minor version; verify with `cargo update --dry-run` in CI |
| Relative paths in `utoipa::path` macros break `ZartApiDoc::openapi()` standalone | Verify in `zart_api_doc_has_paths` test that key relative paths (e.g. `/executions`) are present |
| Env var resolution in `ApiServer::new()` causes test flakiness if env is inherited | Tests use explicit builder calls (`with_api_prefix`) rather than relying on env vars |
| Admin router currently documented as "mount separately" — bringing it into `ApiServer::router()` could surprise users | Document the change clearly; keep `admin_routes::admin_router()` public for those who prefer manual mounting |

## Breaking Changes

- `routes::api_router()` gains a required `prefix: &str` parameter. Direct
  callers (not using `ApiServer`) must pass the prefix explicitly.
- `admin_routes::admin_router()` gains the same parameter.
- `ZartApiDoc` path strings in the OpenAPI output change from absolute
  (`/api/v1/executions`) to relative (`/executions`) when used standalone.
  Users relying on the absolute form for path matching will need to update.
- `ApiServer` users (the common case) are **fully backward-compatible** — defaults
  produce the same routes and OpenAPI paths as spec 0050.

## Definition of Done

- [ ] `just fmt` passes
- [ ] `just lint` passes
- [ ] All unit tests pass (`cargo test` and `cargo test --features openapi`)
- [ ] All integration tests pass including example tests
- [ ] `ApiServer::new(...).router()` with defaults serves routes at `/api/v1/*`
  and `/zart/admin/v1/*` (unchanged from spec 0050)
- [ ] `ApiServer::new(...).with_api_prefix("/v2").router()` serves routes at `/v2/*`
- [ ] With `openapi` feature, `/openapi.json` contains paths matching the
  configured prefix (verified by assertion on path keys in the JSON)
- [ ] `ZART_API_PREFIX` env var is respected when no builder override is given
- [ ] `ZartApiDoc::openapi()` paths are relative and mergeable via `nest`
- [ ] No module in `zart-api/src/` exceeds 700 lines (excluding tests)
- [ ] Website documentation updated to document prefix configuration and env vars

## Notes

- Cross-reference: implements follow-up to spec 0050 (`openapi` feature,
  `ZartApiDoc`, `with_swagger_ui()`).
- If `utoipa-axum` proves incompatible with the current `utoipa 5.x` pin, a
  valid fallback for Phase 4 is to post-process the static `ZartApiDoc::openapi()`
  value: iterate its `paths` map and rewrite keys with the configured prefix.
  This avoids `utoipa-axum` entirely but is more fragile.
- Health check routes (`/healthz`, `/readyz`) and metrics (`/metrics`) are
  intentionally excluded from prefix configuration.
