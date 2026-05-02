# zart-api

Optional Axum HTTP server for the [Zart](https://www.zart.run/) durable execution framework.

Drop `zart-api` into your application to expose execution management over HTTP — no extra infrastructure needed.

## Endpoints

```
GET    /api/v1/executions                         List executions (filterable by status, name)
POST   /api/v1/executions                         Start a new execution
GET    /api/v1/executions/:id                     Get execution status
POST   /api/v1/executions/:id/cancel              Cancel a running execution
GET    /api/v1/executions/:id/wait                Long-poll until completion
POST   /api/v1/events/:id/:event_name             Deliver an external event
GET    /api/v1/stats                              Aggregate counts by status

GET    /zart/admin/v1/executions/:id/detail            Full detail with steps and attempts
POST   /zart/admin/v1/executions/:id/retry-step        Retry a dead step
POST   /zart/admin/v1/executions/:id/restart           Restart an execution from scratch
POST   /zart/admin/v1/executions/:id/rerun             Selective step rerun

GET    /healthz                                   Liveness probe
GET    /readyz                                    Readiness probe
GET    /metrics                                   Prometheus metrics (requires `metrics` feature)
```

## Feature flags

| Flag | Description |
|------|-------------|
| `metrics` | Enables the `/metrics` Prometheus endpoint |
| `openapi` | Annotates all endpoints with utoipa; exports `ZartApiDoc` and `swagger_ui_router()` |

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
