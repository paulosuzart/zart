# Durable AutoAgents (ReAct) Example

Demonstrates **intra-loop durable execution** for an AutoAgents `ReActAgent` using [`zart-autoagents`](../../crates/zart-autoagents).

Unlike `autoagents-agent` (which wraps fixed steps externally with `#[zart_step]`), this example instruments the agent's internal `LLMProvider` and `ToolT` boundaries:

- Each LLM turn is persisted as a Zart step (`llm-call-0`, `llm-call-1`, ...).
- Each tool call is persisted as a Zart step (`tool-fetch_metrics-0`, `tool-lookup_customer-0`, ...).
- If the worker crashes mid-loop, Zart replays completed steps from PostgreSQL without re-calling the LLM or re-executing tools.

## Running

```bash
just up
export GEMINI_API_KEY="your-key-here"

DATABASE_URL=postgres://zart:zart@localhost:5432/zart \
  cargo run -p zart-examples --bin example-autoagents-durable
```
