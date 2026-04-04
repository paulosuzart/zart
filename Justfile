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
    cargo test --workspace -- --include-ignored --test-threads=1

# Run tests for a specific crate only
test-crate crate:
    cargo test -p {{ crate }}

# Run macro tests only (no PostgreSQL required)
test-macros:
    cargo test -p zart-macros

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
    cargo run -p zart-cli -- migrate

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
