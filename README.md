# OpenTalon API Plugin

[![CI](https://github.com/opentalon/api-plugin/actions/workflows/ci.yml/badge.svg)](https://github.com/opentalon/api-plugin/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)

REST API plugin for querying OpenTalon's database — sessions, messages, memories, entities, and usage statistics.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/sessions` | List sessions (`?entity=X&group=Y&limit=20&offset=0`) |
| GET | `/sessions/{id}` | Full session by ID |
| GET | `/sessions/{id}/messages` | Messages from one session (`?last=5&roles=user,assistant`) |
| GET | `/messages` | Chat view across sessions (`?entity=X&group=Y&last=5`) |
| GET | `/memories` | List memories (`?tag=X&actor_id=Y&q=search`) |
| GET | `/memories/{id}` | Single memory |
| GET | `/entities` | List entities (`?group=X`) |
| GET | `/entities/{id}` | Entity by ID |
| GET | `/usage` | Usage records (`?entity=X&group=Y&session=Z&since=2024-01-01`) |
| GET | `/usage/summary` | Aggregated stats (`?entity=X&group=Y&group_by=model_id`) |

### Chat view: `GET /messages?entity=user-123&last=5`

Returns the last 5 request/response pairs (10 messages) for an entity, across all sessions. Defaults to `user` + `assistant` roles.

## Configuration

```yaml
plugins:
  api:
    enabled: true
    github: "opentalon/api-plugin"
    ref: "master"
    expose_http: true
    config:
      api_token: "your-secret-token"  # optional: Bearer token for HTTP auth
```

The DB connection (`__db_driver`, `__db_dsn`) is auto-injected by the host from `state.db` config — no manual setup needed.

Set `OPENTALON_HTTP_PORT` via the plugin's environment to enable the HTTP server (the host reverse-proxies `/api/*` to it).

## Authentication

When `api_token` is set, all HTTP requests require:

```
Authorization: Bearer your-secret-token
```

## Build

```bash
go build -o api-plugin .
```

## Supported databases

- SQLite (read-only WAL mode)
- PostgreSQL (read-only transactions)
