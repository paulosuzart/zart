# Spec 0051 — Configurable Route Prefixes via utoipa-axum

> **Status: stub** — high-level intent only. Full design TBD.

## Context / Problem

Spec 0050 hardcoded `/zart/admin/v1` as the admin route prefix to avoid clashing with
user application paths. The main API prefix is equally fixed at `/api/v1`.

`utoipa-axum`'s `OpenApiRouter` supports runtime prefix nesting:

```rust
let prefix = std::env::var("ZART_ADMIN_PREFIX")
    .unwrap_or_else(|_| "/zart/admin/v1".to_string());

let main_router = OpenApiRouter::new()
    .nest(&prefix, admin_sub_router);

// spec reflects the runtime prefix correctly
let openapi = main_router.into_openapi();
```

This means both the HTTP routes **and** the generated OpenAPI spec can reflect a
user-supplied prefix — something the raw `utoipa::path` macro approach used in spec 0050
cannot do.

## Goals

- Allow users to configure the main API prefix (default: `/api/v1`) and the admin prefix
  (default: `/zart/admin/v1`) without recompiling.
- The Swagger UI and `/openapi.json` must reflect the actual runtime prefixes.
- Defaults must remain backward-compatible with spec 0050.

## Non-Goals (for this spec)

- Per-handler path customisation.
- Prefix changes after server start (static at startup time is fine).

## Approach (sketch)

Migrate `routes.rs` and `admin_routes.rs` from `axum::Router` to
`utoipa_axum::OpenApiRouter` with **relative** handler paths (no prefix in the
`utoipa::path` macro). Apply prefixes at `ApiServer` builder time via:

```rust
ApiServer::new(addr, durable)
    .with_api_prefix("/api/v1")          // or env var
    .with_admin_prefix("/zart/admin/v1") // or env var
    .with_swagger_ui()
    .serve()
    .await?;
```

`ZartApiDoc` generation moves from a static `#[derive(utoipa::OpenApi)]` struct to
`router.into_openapi()` called inside `ApiServer::router()`.

## Open Questions

- Should prefixes come from builder methods, env vars, or both?
- How does `admin_router()` public function change signature — does it take a prefix, or
  does prefix application happen inside `ApiServer` only?
- Can `ZartApiDoc` still be exported as a standalone merge-able type for users who build
  their own router outside `ApiServer`?

## Files Likely Affected

| File | Expected change |
|---|---|
| `crates/zart-api/Cargo.toml` | Add `utoipa-axum` optional dep (under `openapi` feature) |
| `crates/zart-api/src/routes.rs` | Migrate to `OpenApiRouter`; strip prefix from `utoipa::path` |
| `crates/zart-api/src/admin_routes.rs` | Same |
| `crates/zart-api/src/server.rs` | Add prefix builder methods; call `into_openapi()` |
| `crates/zart-api/src/openapi.rs` | Rework or replace `ZartApiDoc` |
| `crates/zart-api/src/lib.rs` | Re-export changes |

## Cross-references

- Implements follow-up to spec 0050 (`openapi` feature, `ZartApiDoc`, `with_swagger_ui()`)
