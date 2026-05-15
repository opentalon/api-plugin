# OpenTalon API Plugin

[![CI](https://github.com/opentalon/api-plugin/actions/workflows/ci.yml/badge.svg)](https://github.com/opentalon/api-plugin/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)

Read-only REST API over OpenTalon's `sessions`, `session_events`, and `prompt_snapshots` tables. Primary consumer is the review UI; aggregations are computed JIT in SQL on each request — no aggregator worker, no denormalised counters.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness + DB ping |
| GET | `/sessions` | Paginated session list, each row with its own JIT-aggregated `stats`, plus `totals` over the full filtered set |
| GET | `/sessions/{id}` | One session with its full message log + structured event log + aggregated `stats` |
| GET | `/events` | Cross-session event list with cursor pagination; optional `include_payload=false` for byte-efficient analytics |
| GET | `/events/stats` | Cross-session aggregates (tokens, cost, counts) — same filters as `/sessions`, no per-session breakdown |
| GET | `/prompt-snapshots?sha=...` | Resolve a prompt body by sha256 (referenced from `turn_start` events) |

### Filters

`/sessions`, `/events`, and `/events/stats` accept the same shared filter set:

- `entity_id` — opentalon entity (in Timly terms: the **user**)
- `group_id` — opentalon group (in Timly terms: the **entity**)
- `since`, `until` — RFC3339 timestamps; left-inclusive, right-exclusive
- `limit` — page size, default 25, capped at 200

`/events` adds:

- `session_id` — restrict to one session
- `event_type` — exact match (e.g. `llm_response`, `tool_call_result`)
- `include_payload=false` — omit payloads (smaller responses, no JOIN cost change)

### Pagination

`/sessions` and `/events` use composite cursor pagination on `(created_at, id)` resp. `(ts, id)`. The response carries `next_cursor` (an opaque base64url blob); pass it back as `?cursor=...` to fetch the next page. Empty `next_cursor` means last page.

### Response shape

```jsonc
// GET /sessions
{
  "items": [
    {
      "id": "...",
      "entity_id": "user_1",
      "group_id": "group_x",
      "summary": "...",
      "active_model": "gpt-4o",
      "created_at": "...",
      "updated_at": "...",
      "stats": {
        "llm_call_count": 5,
        "tool_call_count": 12,
        "tokens_in_total": 5234,
        "tokens_out_total": 1024,
        "cost_input_total": 0.0123,
        "cost_output_total": 0.045
      }
    }
  ],
  "totals": { "session_count": 142, "llm_call_count": 631, "tokens_in_total": ..., ... },
  "next_cursor": "..."
}
```

`/events/stats` returns just the totals block.

## Configuration

```yaml
plugins:
  api:
    enabled: true
    github: "opentalon/api-plugin"
    ref: "master"
    expose_http: true
    db_access: true                    # required: injects core DB credentials
    config:
      api_token: "your-secret-token"  # optional: Bearer token for HTTP auth
```

`db_access: true` tells the host to inject `__db_driver` and `__db_dsn` from the core `state.db` config into the plugin. Without it, the plugin has no database connection.

Set `OPENTALON_HTTP_PORT` via the plugin's environment to enable the HTTP server (the host reverse-proxies `/api/*` to it).

## Authentication

When `api_token` is set, all HTTP requests require:

```
Authorization: Bearer your-secret-token
```

## Quick test

```bash
# Health check (no auth required)
curl -s https://opentalon.example.com/api/health

# List sessions for an entity, last 30 days
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/sessions?entity_id=user_1&since=2024-01-01T00:00:00Z&limit=25'

# Cross-session cost rollup for a group
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events/stats?group_id=team-a&since=2024-01-01T00:00:00Z'

# All llm_response events in a session, payload-light
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events?session_id=sess_a&event_type=llm_response&include_payload=false'
```

## Build

```bash
go build -o api-plugin .
```

## Supported databases

- SQLite (read-only WAL mode)
- PostgreSQL (read-only transactions)

## Plugin tool surface

The plugin does **not** advertise gRPC tool actions: data access is HTTP-only by design. The sibling `mcp-plugin` is the LLM-facing tool entry point.
