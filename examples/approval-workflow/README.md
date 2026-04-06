# Approval Workflow Example

Demonstrates a **human-in-the-loop durable execution** that pauses for an external event before continuing.

## Features Used

- **`#[zart_step]` macro** — defines step functions as standalone async functions
- **`ctx.execute_step()`** — executes steps with automatic retry/timeout handling
- **`wait_for_event`** — suspends execution until an external event is delivered
- **Event delivery via `offer_event`** — resumes the waiting execution with a typed payload
- **Sequential steps** — steps before and after the event wait, passing data between them

## Flow

1. **Validate request** — a fake step that checks the approval request
2. **Wait for manager approval** — pauses execution until an approval event is delivered
3. **On approval** — processes the request and returns a result
4. **On rejection** — returns a rejection notice

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-approval-workflow
```

The example simulates the approval by delivering the event after a short delay (as if a manager reviewed the request). In a real system, the event would come from an HTTP API call or CLI command.

## What You'll See

```
=== Zart Approval Workflow Example ===

Starting execution 'approval-demo-...'...
Worker started. Execution will wait for approval...
Execution is waiting for manager approval...
Delivering approval event...
Approval received! Processing approved request...
Execution completed!
Decision: approved
Reviewer: Manager Carol
```

## Key Concept: `wait_for_event`

The `wait_for_event` API blocks the durable execution. The task is parked in the database with a far-future execution time. When `offer_event` is called (from an API, CLI, or another system), the task is woken up and receives the event payload.

This pattern enables:
- Manager approval workflows
- Interactive onboarding flows
- Integration with external review systems
- Human-in-the-loop AI agent chains
