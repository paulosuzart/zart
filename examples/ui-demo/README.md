# ui-demo

Backend fixture for the [Zart](https://www.zart.run/) Admin UI.

Starts a `PostgresScheduler`, registers two task types, seeds a handful of executions, then runs the `zart-api` HTTP server on port `3000` alongside a worker until Ctrl+C. Designed to be used together with the `zart-ui` frontend.

## Run

```bash
# Start PostgreSQL
docker compose up -d postgres

# Run the backend
cargo run -p ui-demo

# Open the Admin UI (in a separate terminal)
cd zart-ui && npm run dev
# Set the API Server to http://localhost:3000
```

Requires `DATABASE_URL` to point at a running PostgreSQL instance.

---

Part of the [Zart](https://www.zart.run/) repository — <https://github.com/paulosuzart/zart>
