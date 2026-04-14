# CLI Demo Example

Demonstrates **Zart CLI admin commands** interacting with a long-running durable execution. This example shows how to use the `zart` CLI to monitor, pause, resume, restart, and inspect durable executions while they run.

## Features Demonstrated

- **`zart status <execution_id>`** — Check execution status in real-time
- **`zart pause --execution-id <id>`** — Create pause rules to temporarily halt execution
- **`zart resume --execution-id <id>`** — Resume execution by soft-deleting pause rules
- **`zart pause-list`** — List all pause rules (active and deleted)
- **`zart restart <execution_id>`** — Full restart with history preservation
- **`zart runs <execution_id>`** — List all past runs from the run history

## Flow

The `demo.sh` script handles everything automatically:

1. Generates a unique execution ID (e.g., `cli-demo-add3f0e5-728f`)
2. Starts the Rust durable execution in the background
3. Waits for the execution to initialize in the database
4. Runs through a sequence of CLI commands demonstrating each admin feature
5. Cleans up the background process on exit

The durable execution follows this timeline:

```
┌─────────────────────────────────────────────────────────┐
│  Rust Process (background, ~2 minutes total)             │
│  ┌──────────┐  ┌──────┐  ┌──────────┐  ┌──────┐        │
│  │ prepare  │→│sleep │→│ process  │→│sleep │→│finalize│
│  │ (2s)     │  │(30s) │  │ (2s)     │  │(30s) │  │(2s)  │
│  └──────────┘  └──────┘  └──────────┘  └──────┘        │
│         ↑           ↑                        ↑           │
│         └─ CLI ─────┴─── CLI commands ───────┘           │
└─────────────────────────────────────────────────────────┘
```

CLI commands execute during the sleep windows, demonstrating pause/resume, restart, and run history.

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Run the CLI demo (fully automated)
just example-cli-demo
```

## What You'll See

```
╔══════════════════════════════════════════════════════════╗
║        Zart CLI Admin Commands — Interactive Demo       ║
╚══════════════════════════════════════════════════════════╝

Execution ID: cli-demo-add3f0e5-728f-47cd
Database:     postgres://zart:zart@localhost:5432/zart

Starting durable execution in background...
✓ Rust process started (PID: 70065)

Waiting for execution to initialize...
✓ Execution initialized (2 seconds)

▶ Check execution status
  $ cargo run -q -p zart-cli -- --database-url postgres://zart:zart@localhost:5432/zart status cli-demo-add3f0e5-728f-47cd

execution_id : cli-demo-add3f0e5-728f-47cd
task_name    : zart::cli_demo::CliDemoTask
status       : Running
scheduled_at : 2026-04-14T00:45:00.000000+00:00

─────────────────────────────────────────────────────────

Creating pause rule...

▶ Create pause rule for this execution
  $ cargo run -q -p zart-cli -- --database-url postgres://zart:zart@localhost:5432/zart pause --execution-id cli-demo-add3f0e5-728f-47cd --triggered-by demo-script

Created pause rule 'rule-abc123' (...)

─────────────────────────────────────────────────────────

▶ List active pause rules
  $ cargo run -q -p zart-cli -- --database-url postgres://zart:zart@localhost:5432/zart pause-list

rule-abc123 PauseScope { execution_id: Some("cli-demo-..."), ... }

─────────────────────────────────────────────────────────

... (continues with resume, restart, runs, etc.)

╔══════════════════════════════════════════════════════════╗
║              CLI Demo Commands Complete                  ║
╚══════════════════════════════════════════════════════════╝

Commands demonstrated:
  • zart status         — Check execution status
  • zart pause          — Create pause rules
  • zart resume         — Soft-delete pause rules
  • zart pause-list     — List all pause rules
  • zart restart        — Full restart with history preservation
  • zart runs           — List run history

Background process will be cleaned up automatically.
```

## Key Concepts

### Pause Rules
Pause rules are **scheduling-time controls** — they prevent new tasks from being scheduled without affecting in-flight work. When you run `zart pause`, a rule is inserted into `zart_pause_rules`. The worker checks this table before scheduling the next task and silently stops if a rule matches.

`zart resume` soft-deletes the rules (sets `deleted_at`), allowing scheduling to continue. The execution replays naturally from `zart_steps` — no state loss occurs.

### Run History
Every restart creates a new run row in `zart_execution_runs`. The original run is preserved with its status, and a new run begins with `trigger = 'restart'`. Run history is append-only and fully auditable.

### CLI vs Embedded API
This example demonstrates the **CLI tier** of admin access. The same operations are available programmatically via `DurableScheduler` methods (see `admin-demo` example) or via HTTP through `zart-api`'s `admin_router()`.

## Manual Exploration

While the demo script runs automatically, you can also interact manually:

```bash
# In one terminal: start the long-running execution
DATABASE_URL=postgres://zart:zart@localhost:5432/zart \
cargo run -p example-cli-demo

# Note the execution ID from the output, then in another terminal:
export DATABASE_URL=postgres://zart:zart@localhost:5432/zart
export EXECUTION_ID=<the-id-from-above>

# Check status
cargo run -q -p zart-cli -- status $EXECUTION_ID

# Pause execution
cargo run -q -p zart-cli -- pause --execution-id $EXECUTION_ID --triggered-by me

# List pause rules
cargo run -q -p zart-cli -- pause-list

# Resume
cargo run -q -p zart-cli -- resume --execution-id $EXECUTION_ID --triggered-by me

# Restart and check runs
cargo run -q -p zart-cli -- restart $EXECUTION_ID --payload '{"fail_step":false}'
cargo run -q -p zart-cli -- runs $EXECUTION_ID
```
