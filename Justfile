# Zart — Justfile
# Run `just --list` to see all available commands.

# Default: show available recipes
default:
    @just --list

# ── Build ──────────────────────────────────────────────────────────────────────

# Build all workspace crates in debug mode
build:
    cargo build --workspace

# Build all workspace crates in release mode
build-release:
    cargo build --workspace --release

# Check all workspace crates without producing artifacts (faster than build)
check:
    cargo check --workspace

# ── Test ───────────────────────────────────────────────────────────────────────

# Run unit tests for all crates (skips tests marked #[ignore])
test:
    cargo test --workspace

# Run all tests including those that require a running PostgreSQL instance.
# Uses --test-threads=1 so integration tests don't race each other via SKIP LOCKED.
test-integration:
    cargo test --workspace --features scheduler/postgres -- --include-ignored --test-threads=1

# Run integration tests (PostgreSQL required, internet NOT required)
# Excludes example tests that call external APIs
test-integration-core:
    cargo test -p scheduler --test integration_test --features postgres -- --include-ignored --test-threads=1
    cargo test -p zart --test integration_test -- --include-ignored --test-threads=1

# Run tests for a specific crate only
test-crate crate:
    cargo test -p {{ crate }}

# Run macro tests only (no PostgreSQL required)
test-macros:
    cargo test -p zart-macros

# Run observability-specific tests (metrics, tracing, logging)
test-observability:
    cargo test -p zart metrics
    cargo test -p zart logging
    cargo test -p zart-api metrics
    cargo test -p zart-api healthz
    cargo test -p zart-api readyz

# ── Lint ───────────────────────────────────────────────────────────────────────

# Run clippy on all crates with strict settings
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Format all source files
fmt:
    cargo fmt --all

# Check formatting without modifying files (used in CI)
fmt-check:
    cargo fmt --all -- --check

# ── Documentation ──────────────────────────────────────────────────────────────

# Generate and open crate documentation in a browser
doc:
    cargo doc --workspace --no-deps --open

# Generate documentation without opening (for CI)
doc-check:
    cargo doc --workspace --no-deps

# ── Docker / Database ──────────────────────────────────────────────────────────

# Start PostgreSQL via Docker Compose
up:
    docker compose up -d postgres
    @echo "Waiting for PostgreSQL to be ready..."
    @until docker compose exec -T postgres pg_isready -U zart -d zart > /dev/null 2>&1; do sleep 1; done
    @echo "PostgreSQL is ready."

# Stop and remove Docker Compose services
down:
    docker compose down

# Stop services and remove volumes (destructive — deletes all data)
down-clean:
    docker compose down -v

# Show logs for the PostgreSQL container
logs:
    docker compose logs -f postgres

# ── Database Migrations ────────────────────────────────────────────────────────

# Run database migrations (requires PostgreSQL to be running)
migrate:
    DATABASE_URL=postgres://zart:zart@localhost:5432/zart cargo run -p zart-cli -- migrate

# ── Combined Workflows ─────────────────────────────────────────────────────────

# Full CI check: format, lint, build, and test (no PostgreSQL required)
ci:
    just fmt-check
    just lint
    just build
    just test

# Start dependencies and run integration tests
ci-integration:
    just up
    DATABASE_URL=postgres://zart:zart@localhost:5432/zart just test-integration
    just down
    @echo "Integration tests completed successfully!"

# ── Observability ──────────────────────────────────────────────────────────────

# Start a development server with observability enabled
# Usage: just dev-server [PORT]
dev-server port='8080':
    @echo "Starting Zart API server on port {{port}} with observability"
    @echo "Metrics available at: http://localhost:{{port}}/metrics"
    @echo "Health checks: http://localhost:{{port}}/healthz, http://localhost:{{port}}/readyz"
    RUST_LOG=info cargo run -p zart-api -- --port {{port}}

# Generate a Grafana dashboard JSON for Prometheus metrics
generate-grafana-dashboard:
    @echo "Grafana dashboard generation not yet implemented"
    @echo "Metrics available:"
    @echo "  - zart_tasks_total"
    @echo "  - zart_task_duration_seconds"
    @echo "  - zart_steps_total"
    @echo "  - zart_step_duration_seconds"
    @echo "  - zart_queue_depth"
    @echo "  - zart_worker_concurrent_tasks"
    @echo "  - zart_poll_interval_seconds"
    @echo "  - zart_executions_total"
    @echo "  - zart_events_delivered_total"

# ── Examples (M9) ─────────────────────────────────────────────────────────────

# Run the brewery-finder example (sequential steps, macros, structured output)
# Usage: just example-brewery-finder [DATABASE_URL]
example-brewery-finder db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-brewery-finder

# Run the brewery-finder-step-fn example (#[zart_step] macro, standalone step functions)
# Usage: just example-brewery-finder-step-fn [DATABASE_URL]
example-brewery-finder-step-fn db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-brewery-finder-step-fn

# Run the approval-workflow example (human-in-the-loop with wait_for_event)
# Usage: just example-approval [DATABASE_URL]
example-approval db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-approval-workflow

# Run the parallel-steps example (schedule_step + wait_all)
# Usage: just example-parallel [DATABASE_URL]
example-parallel db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-parallel-steps

# Run the radkit-agent example (AI-powered workflow with LLM integration)
# Usage: just example-radkit-agent [DATABASE_URL]
example-radkit-agent db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-radkit-agent

# Run the retry-simulation example (demonstrates intentional failure and automatic retry)
# Usage: just example-retry-simulation [DATABASE_URL]
example-retry-simulation db_url='postgres://zart:zart@localhost:5432/zart':
    just migrate
    RUST_LOG=info DATABASE_URL={{db_url}} cargo run -p zart-examples --bin example-retry-simulation

# Run all examples sequentially (requires PostgreSQL and internet)
run-all-examples db_url='postgres://zart:zart@localhost:5432/zart':
    just example-brewery-finder {{db_url}}
    just example-brewery-finder-step-fn {{db_url}}
    just example-approval {{db_url}}
    just example-parallel {{db_url}}
    just example-radkit-agent {{db_url}}
    just example-retry-simulation {{db_url}}

# Run integration tests for examples (requires PostgreSQL and internet)
test-examples:
    cargo test -p zart-examples -- --include-ignored --test-threads=1

# Check that examples compile without running them
check-examples:
    cargo check -p zart-examples --all-targets
