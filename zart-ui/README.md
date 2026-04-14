# Zart UI

React-based frontend for Zart.

## Quick Start

### Docker

Pull and run:

```bash
docker run -d \
  --name zart-ui \
  -p 8080:80 \
  -e API_URL=http://localhost:3000 \
  paulosuzart/zart-ui:latest
```

Then open http://localhost:8080

### Docker Compose (local development)

```bash
docker compose up -d
```

Opens on http://localhost:8080. The UI proxies `/api/` and `/admin/` to `http://host.docker.internal:3000`.

### Override API URL

| Method | How |
|--------|-----|
| Docker run | `-e API_URL=https://api.example.com` |
| Docker compose | `API_URL=https://api.example.com docker compose up` |
| Build-time arg | `--build-arg API_URL=https://api.example.com` |

The API URL must include the scheme (`http://` or `https://`). Trailing slash optional.

## Image Tags

| Tag | When to Use |
|-----|-------------|
| `latest` | Most recent build |
| `0.1.0`, etc. | Specific release |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_URL` | `http://host.docker.internal:3000` | Base URL of the Zart API backend |
