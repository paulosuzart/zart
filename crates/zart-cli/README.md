# zart-cli

Command-line interface for the [Zart](https://www.zart.run/) durable execution framework.

```
cargo install zart-cli
```

## Commands

| Command | Description |
|---------|-------------|
| `zart migrate` | Apply database migrations |
| `zart schedule` | Schedule a task for immediate execution |
| `zart status` | Inspect an execution's current status and steps |
| `zart cancel` | Cancel a running execution |
| `zart wait` | Block until an execution completes |
| `zart retry-step` | Retry a dead step within the current run |
| `zart restart` | Restart an entire execution from scratch |
| `zart rerun` | Selectively rerun a subset of steps |
| `zart pause` | Create a pause rule to suspend task processing |
| `zart resume` | Remove a pause rule |
| `zart pause-list` | List active pause rules |

## Configuration

Set `DATABASE_URL` (or pass `--database-url`) pointing to the PostgreSQL instance used by your Zart workers.

```bash
export DATABASE_URL=postgres://user:pass@localhost:5432/mydb
zart migrate
zart status <execution-id>
```

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
