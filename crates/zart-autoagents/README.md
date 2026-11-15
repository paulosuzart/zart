# zart-autoagents

Durable execution harness integrating [AutoAgents](https://github.com/liquidos-ai/AutoAgents) with [Zart](https://github.com/paulosuzart/zart).

`zart-autoagents` persists every LLM reasoning turn and every tool invocation as deterministic Zart steps in PostgreSQL, allowing ReAct agents to survive crashes, restarts, and long human-in-the-loop waits without re-executing completed LLM calls or tools.

## Features

- **`DurableReactTask<A>`**: Drives an AutoAgents `ReActAgent` loop directly on Zart primitives (`zart::require` for `llm-call-{turn}`, `zart::schedule` + `zart::wait` for parallel `tool-{turn}-{idx}-{name}` execution).
- **`DurableLLMProvider` & `DurableChatResponse`**: Serializable LLM wrapper that checkpoints `chat` and `chat_with_tools` calls.
- **`DurableTool`**: Wraps `autoagents::core::tool::ToolT` inside durable Zart steps.
- **`DurableAgent<T>` & `ControlFlowSlot`**: Bridges AutoAgents error types with Zart's `StepError::Scheduled` suspension signal via `run_durable_agent`.
- **`wait_for_hitl`**: Durable human-in-the-loop pause/resume built on `zart::wait_for_event`.

## Example

See [`examples/autoagents-durable`](../../examples/autoagents-durable) for a complete multi-tool SRE incident investigation agent with parallel tool execution.

## Roadmap: Full AutoAgents & Actors Compatibility

`zart-autoagents` aims for full compatibility with AutoAgents' multi-agent and **Actor** runtime:

- **Durable `ActorAgent` Execution**: Long-lived actors backed by Zart `execution_id`s with checkpointed state across restarts.
- **Durable Message Passing**: Actor-to-actor mailboxes and pub/sub powered by `zart::schedule` and `zart::wait_for_event`.
- **Multi-Agent Supervision**: Parallel fan-out/fan-in (`zart::wait`), per-actor retries, and execution deadlines.
