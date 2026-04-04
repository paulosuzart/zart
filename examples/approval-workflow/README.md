# Approval Workflow Example

Demonstrates a **human-in-the-loop durable execution** that pauses for an external event before continuing.

## Features Used

- **`wait_for_event`** — suspends execution until an external event is delivered
- **Event delivery via `offer_event`** — resumes the waiting execution with a typed payload
- **Sequential steps** — steps before and after the event wait, passing data between them
- **External API calls** — uses the Zippopotamus and Open Brewery DB APIs
- **Result file output** — writes the approval decision and brewery data to a temp file

## Flow

1. **Fetch location data** — calls the Zippopotamus API to look up a ZIP code
2. **Wait for approval** — pauses execution until a manager approves (simulated via `offer_event`)
3. **On approval** — queries the Open Brewery DB for breweries in the area and writes a "recommendations" file
4. **On rejection** — writes a rejection notice to a temp file

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
cargo run -p zart-examples --bin example-approval-workflow
```

The example simulates the approval by delivering the event after a short delay (as if a manager reviewed the request). In a real system, the event would come from an HTTP API call or CLI command.

## What You'll See

```
=== Zart Approval Workflow Example ===

Starting execution 'approval-demo-1'...
Worker started. Execution will wait for approval...
Execution is waiting for manager approval...
Delivering approval event...
Approval received! Fetching brewery recommendations...
Execution completed!
Decision: approved
Breweries found: 5
Report written to: /tmp/zart-approval-XXXXXX.txt
```

## Key Concept: `wait_for_event`

The `wait_for_event` API blocks the durable execution. The task is parked in the database with a far-future execution time. When `offer_event` is called (from an API, CLI, or another system), the task is woken up and receives the event payload.

This pattern enables:
- Manager approval workflows
- Interactive onboarding flows
- Integration with external review systems
- Human-in-the-loop AI agent chains
