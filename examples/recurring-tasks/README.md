# Recurring Tasks Example

Demonstrates Zart's recurring task support using `schedule_recurring` and worker auto-rescheduling.

## Usage

```bash
# Start PostgreSQL (if not running)
docker compose up -d postgres

# Run the example
cd examples/recurring-tasks
cargo run
```

## What it does

1. Schedules a `heartbeat-check` task that runs every 2 seconds (FixedDelay)
2. Schedules a `daily-report` task that runs every minute at :30 seconds (Cron)
3. Runs the worker for 5 seconds to observe recurring executions
4. Tasks automatically reschedule without explicit handler logic
