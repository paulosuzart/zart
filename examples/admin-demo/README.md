# admin-demo

Example demonstrating the [Zart](https://www.zart.run/) admin operations API.

Shows how to use the advanced lifecycle controls on a running durable workflow:

- `wait_completion<T>` — typed result deserialization
- `start_and_wait_for` — start and wait in a single call
- `restart` — full execution restart with history preserved
- `retry_step` — retry a single dead step within the current run
- `rerun_steps` — selective step rerun with dependency resolution
- `pause` / `resume` / `list_pause_rules` — suspend and resume task processing

## Run

```bash
# Start PostgreSQL
docker compose up -d postgres

# Run the example
cargo run -p admin-demo
```

Requires `DATABASE_URL` to point at a running PostgreSQL instance.

---

Part of the [Zart](https://www.zart.run/) repository — <https://github.com/paulosuzart/zart>
