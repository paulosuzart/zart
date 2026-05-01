//! OpenAPI documentation for the Zart HTTP API.
//!
//! This module is only compiled when the `openapi` feature is enabled.
//! It exports [`ZartApiDoc`], a type that implements [`utoipa::OpenApi`] and
//! can be merged into an existing utoipa API doc via the `nest` attribute.
//!
//! # Example — merge into your own API doc
//!
//! ```rust,ignore
//! use zart_api::openapi::ZartApiDoc;
//!
//! #[derive(utoipa::OpenApi)]
//! #[openapi(
//!     nest(
//!         (path = "/", api = ZartApiDoc)
//!     )
//! )]
//! struct MyAppDoc;
//! ```

use crate::models::{
    ErrorResponse, ExecutionDetailResponse, ExecutionResponse, ListQuery, PauseRequest,
    PauseRuleResponse, PotentiallyStaleDepResponse, RerunRequest, RerunResponse, RestartRequest,
    RestartResponse, ResumeResponse, RetryStepRequest, RetryStepResponse, RunRecordResponse,
    StartExecutionRequest, StartExecutionResponse, StatsResponse, StepAttemptResponse,
    StepDetailResponse, WaitQuery,
};
use crate::{admin_routes, routes};

/// OpenAPI 3.x schema document for the Zart HTTP API.
///
/// All paths are **relative** (e.g., `/executions`, `/pause`). When embedding
/// this doc via the utoipa `nest` attribute, supply the desired prefix there.
/// To get a fully-prefixed `OpenApi` value at runtime, use [`build_openapi`].
///
/// Use the utoipa `nest` attribute to incorporate this schema into an existing
/// application-level `ApiDoc`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        routes::list_executions,
        routes::start_execution,
        routes::get_execution,
        routes::cancel_execution,
        routes::wait_execution,
        routes::offer_event,
        routes::get_stats,
        routes::healthz,
        routes::readyz,
        admin_routes::retry_step,
        admin_routes::restart,
        admin_routes::rerun,
        admin_routes::execution_detail,
        admin_routes::list_runs,
        admin_routes::create_pause,
        admin_routes::list_pauses,
        admin_routes::resume_rule,
        admin_routes::delete_pause_rule,
    ),
    components(schemas(
        StartExecutionRequest,
        ListQuery,
        WaitQuery,
        ExecutionResponse,
        StartExecutionResponse,
        ErrorResponse,
        RetryStepRequest,
        RestartRequest,
        RerunRequest,
        RetryStepResponse,
        RestartResponse,
        RerunResponse,
        PotentiallyStaleDepResponse,
        RunRecordResponse,
        PauseRequest,
        PauseRuleResponse,
        ResumeResponse,
        StatsResponse,
        ExecutionDetailResponse,
        StepDetailResponse,
        StepAttemptResponse,
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

// ── Internal split docs ───────────────────────────────────────────────────────
// Used only by `build_openapi` to apply per-group prefixes without guessing
// which paths belong to which router.

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    routes::list_executions,
    routes::start_execution,
    routes::get_execution,
    routes::cancel_execution,
    routes::wait_execution,
    routes::offer_event,
    routes::get_stats,
    routes::healthz,
    routes::readyz,
))]
struct ApiOnlyDoc;

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    admin_routes::retry_step,
    admin_routes::restart,
    admin_routes::rerun,
    admin_routes::execution_detail,
    admin_routes::list_runs,
    admin_routes::create_pause,
    admin_routes::list_pauses,
    admin_routes::resume_rule,
    admin_routes::delete_pause_rule,
))]
struct AdminOnlyDoc;

// ── Prefix rewriter ───────────────────────────────────────────────────────────

const DEFAULT_API_SKIP: &[&str] = &["/healthz", "/readyz"];
const DEFAULT_ADMIN_SKIP: &[&str] = &[];

/// Rewrites all path keys in an [`utoipa::openapi::OpenApi`] by prepending
/// `prefix`, except for paths listed in `skip` (kept at the root).
struct PrefixRewriter {
    prefix: String,
    skip: Vec<String>,
}

impl utoipa::Modify for PrefixRewriter {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let old = std::mem::take(&mut openapi.paths.paths);
        for (path, item) in old {
            let new_path = if self.skip.iter().any(|s| s == &path) {
                path
            } else {
                format!("{}{}", self.prefix, path)
            };
            openapi.paths.paths.insert(new_path, item);
        }
    }
}

fn resolve_skip(provided: Option<&[&str]>, default: &[&str]) -> Vec<String> {
    provided
        .unwrap_or(default)
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Build a fully-prefixed [`utoipa::openapi::OpenApi`] value.
///
/// Structurally separates API and admin path groups, then applies prefixes via
/// an internal [`utoipa::Modify`] implementation.
///
/// `api_skip` and `admin_skip` list paths that should stay at the root (no
/// prefix applied). When `None`, the compiled defaults are used
/// (`/healthz,/readyz` for the API group, empty for admin). Pass `Some(&[])`
/// to disable all skipping, or `Some(&["/healthz"])` to customise the list.
/// To drive skip lists from env vars, resolve them before calling and pass as
/// `Some(paths)`.
///
/// Components, tags, and info are taken from [`ZartApiDoc`] so the merged
/// result is fully documented.
///
/// Use this when constructing a Swagger UI outside of [`crate::server::ApiServer`].
pub fn build_openapi(
    api_prefix: &str,
    admin_prefix: &str,
    api_skip: Option<&[&str]>,
    admin_skip: Option<&[&str]>,
) -> utoipa::openapi::OpenApi {
    use utoipa::{Modify as _, OpenApi as _};

    let mut api_doc = ApiOnlyDoc::openapi();
    PrefixRewriter {
        prefix: api_prefix.to_string(),
        skip: resolve_skip(api_skip, DEFAULT_API_SKIP),
    }
    .modify(&mut api_doc);

    let mut admin_doc = AdminOnlyDoc::openapi();
    PrefixRewriter {
        prefix: admin_prefix.to_string(),
        skip: resolve_skip(admin_skip, DEFAULT_ADMIN_SKIP),
    }
    .modify(&mut admin_doc);

    for (path, item) in admin_doc.paths.paths {
        api_doc.paths.paths.insert(path, item);
    }

    // Carry over components, tags, and info from the full combined doc.
    let full = ZartApiDoc::openapi();
    api_doc.components = full.components;
    api_doc.tags = full.tags;
    api_doc.info = full.info;

    api_doc
}

/// Build an Axum router that serves the Swagger UI at `/swagger-ui` and the
/// OpenAPI schema at `/openapi.json`.
///
/// Prefixes are supplied explicitly so that env var resolution (if needed)
/// happens once at the call site — typically where the [`ApiServer`] or the
/// application router is constructed — rather than inside this function.
///
/// Merge this into your existing router to expose the docs without pulling in
/// `utoipa` or `utoipa-swagger-ui` directly.
///
/// # Example
///
/// ```rust,ignore
/// let router = my_router().merge(zart_api::openapi::swagger_ui_router("/api/v1", "/zart/admin/v1"));
/// ```
///
/// [`ApiServer`]: crate::server::ApiServer
pub fn swagger_ui_router(api_prefix: &str, admin_prefix: &str) -> axum::Router {
    utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
        .url(
            "/openapi.json",
            build_openapi(api_prefix, admin_prefix, None, None),
        )
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi as _;

    #[test]
    fn zart_api_doc_has_paths() {
        let doc = ZartApiDoc::openapi();
        assert!(
            !doc.paths.paths.is_empty(),
            "ZartApiDoc must expose at least one path"
        );
    }

    #[test]
    fn zart_api_doc_paths_are_relative() {
        let doc = ZartApiDoc::openapi();
        for path in doc.paths.paths.keys() {
            assert!(
                !path.starts_with("/api/v1") && !path.starts_with("/zart/admin"),
                "path '{path}' must be relative (no hardcoded prefix)"
            );
        }
    }

    #[test]
    fn build_openapi_applies_custom_api_prefix() {
        let doc = build_openapi("/v2", "/zart/admin/v1", None, None);
        assert!(
            doc.paths.paths.contains_key("/v2/executions"),
            "expected /v2/executions in paths"
        );
    }

    #[test]
    fn build_openapi_applies_custom_admin_prefix() {
        let doc = build_openapi("/api/v1", "/ops/admin", None, None);
        assert!(
            doc.paths.paths.contains_key("/ops/admin/pause"),
            "expected /ops/admin/pause in paths"
        );
    }

    #[test]
    fn build_openapi_keeps_health_at_root() {
        let doc = build_openapi("/v2", "/ops", None, None);
        assert!(
            doc.paths.paths.contains_key("/healthz"),
            "/healthz must remain at root regardless of prefix"
        );
        assert!(
            doc.paths.paths.contains_key("/readyz"),
            "/readyz must remain at root regardless of prefix"
        );
    }

    #[test]
    fn build_openapi_no_unprefixed_api_paths() {
        let doc = build_openapi("/v2", "/ops", None, None);
        for path in doc.paths.paths.keys() {
            let is_root = path == "/healthz" || path == "/readyz";
            let has_prefix = path.starts_with("/v2") || path.starts_with("/ops");
            assert!(
                is_root || has_prefix,
                "path '{path}' has no prefix — expected /v2 or /ops"
            );
        }
    }

    #[test]
    fn build_openapi_custom_api_skip_overrides_default() {
        // Pass an empty skip list — healthz/readyz should now be prefixed
        let doc = build_openapi("/v2", "/ops", Some(&[]), None);
        assert!(
            doc.paths.paths.contains_key("/v2/healthz"),
            "/v2/healthz must appear when skip list is empty"
        );
        assert!(
            !doc.paths.paths.contains_key("/healthz"),
            "/healthz must not appear at root when skip list is empty"
        );
    }

    #[test]
    fn build_openapi_no_unprefixed_admin_paths() {
        let doc = build_openapi("/api/v1", "/ops/admin", None, None);
        // /pause should only appear under the admin prefix, never bare
        assert!(
            !doc.paths.paths.contains_key("/pause"),
            "/pause must not appear unprefixed"
        );
        assert!(
            doc.paths.paths.contains_key("/ops/admin/pause"),
            "/ops/admin/pause must be present"
        );
    }

    #[test]
    fn zart_api_doc_serialises_to_json() {
        let doc = ZartApiDoc::openapi();
        let json = serde_json::to_string(&doc).expect("serialisation must succeed");
        assert!(json.contains("Zart API"), "title must appear in JSON");
    }
}
