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
| GET | `/sessions/{id}/events` | Incremental tail-poll for one session — events with `seq > since_seq` in ASC order, designed for a 2 s polling loop after the initial `/sessions/{id}` envelope fetch |
| GET | `/events` | Cross-session event list with cursor pagination; optional `include_payload=false` for byte-efficient analytics |
| GET | `/events/stats` | Cross-session aggregates (tokens, cost, counts) — same filters as `/sessions` plus optional `event_type` exact-match, optional `group_by=event_type` + `sample_sessions=N`, optional `bucket_by=day/week/month/year` for time-series |
| GET | `/prompt-snapshots?sha=...` | Resolve a prompt body by sha256 (referenced from `turn_start` events) |

### Filters

`/sessions`, `/events`, and `/events/stats` accept the same shared filter set:

- `entity_id` — opentalon entity (in Timly terms: the **user**); singular
  form, kept for backward compat. Used alone, it scopes to the same row
  set as `include_entity_ids=<id>`; when both are set the two predicates
  AND together.
- `group_id` — opentalon group (in Timly terms: the **entity**)
- `include_entity_ids` — comma-separated list of entity_ids to scope rows
  **and** aggregation to. Use it for multi-actor scope (e.g. a support
  engineer reviewing sessions across a handful of related users) instead
  of N round-trips with `entity_id`. ANDs with the singular `entity_id`
  if both are set. Empty value and unknown ids are no-ops; capped at 200.
- `exclude_entity_ids` — comma-separated list of entity_ids to exclude
  from rows **and** aggregation. Use it to keep list, totals, and
  `/events/stats` numbers in lockstep when hiding a subset of actors
  (e.g. support staff who shouldn't count against a tenant's cost view).
  Empty value and unknown ids are no-ops; capped at 200. ANDs with
  `include_entity_ids` if both are set.
- `since`, `until` — RFC3339 timestamps; left-inclusive, right-exclusive. **Filter on event timestamp uniformly across all endpoints**: a session with `created_at` before the window but events inside is included; a session with `created_at` inside the window but events outside is not. Containers vs activity — the API tracks activity.
- `limit` — page size, default 25, capped at 200. Any value outside `(1..200]` → 400 (matches the strictness of every other cap in the API). To page through more rows, use the returned `next_cursor`.

`/events` adds:

- `session_id` — restrict to one session
- `event_type` — exact match (e.g. `llm_response`, `tool_call_result`)
- `include_payload=false` — omit payloads (smaller responses, no JOIN cost change). Accepts literal `true` / `false` only; any other value (`banana`, `1`, `TRUE`, …) → 400.

`/events/stats` adds:

- `event_type` — exact match, narrows totals and time buckets to a single event type. Sub-counters for the other types zero out (e.g. `event_type=tool_call_result` → `llm_call_count=0`, `tokens_*=0`). Mutually exclusive with `group_by=event_type` (the grouping would collapse to one bucket — 400 instead).

### Sort (`/sessions`)

`/sessions` accepts `sort` + `direction` for analytics use cases ("top N sessions by cost", "highest tool-call counts"). Defaults preserve the pre-#8 contract.

- `sort` (default `created_at`) — one of: `created_at`, `updated_at`, `cost_total`, `llm_call_count`, `tool_call_count`, `tokens_in_total`, `tokens_out_total`. `cost_total` is `cost_input_total + cost_output_total`. `updated_at` exists for the session-picker case ("most recently active sessions"), separate from creation time.
- `direction` (default `desc`) — `asc` or `desc`.

The cursor format extends to four fields — `<sort>|<direction>|<value>|<id>`, base64url-encoded — so the active sort identifier travels with the boundary. Replaying a cursor under a different `sort`/`direction` returns 400 ("cursor sort mismatch") rather than walking a meaningless keyset. Legacy two-field cursors (`<value>|<id>`) minted before this feature are accepted under the default sort for back-compat.

### Pagination

`/sessions` and `/events` use composite cursor pagination on `(sort-key-value, id)`. The response carries `next_cursor` (an opaque base64url blob); pass it back as `?cursor=...` to fetch the next page. Empty `next_cursor` means last page.

### Tail polling — `/sessions/{id}/events`

Purpose-built for the live-tail view on the AI-Sessions diagnostic page: after an initial `GET /sessions/{id}` envelope fetch, the consumer polls this endpoint on a ~2 s cadence to receive events as they're written.

- `since_seq=N` (default `0`) — returns events with `seq > N` in **ascending seq order** (oldest → newest). Append directly to the rendered log.
- `limit` — standard limit clamp (default 25, max 200; out-of-bounds → 400).
- `include_payload` — same `true`/`false` semantics as `/events`.

**Cursor is `seq` itself.** No opaque token, no `next_cursor` field on this surface. The client tracks `max(items[].seq)` from each response and passes it as the next `since_seq`. The (`session_id`, `seq`) unique index in opentalon-core guarantees per-session monotonic, gap-free seq starting at 1 — once a client has seen seq=N, no event with `seq < N` will appear in any future poll for that session.

**Burst handling.** If `items.length === limit`, immediately re-poll with the new `since_seq` (skip the regular interval). Repeat until `items.length < limit`, then resume the normal 2 s cadence.

**Idempotent overshoot.** A `since_seq` ≥ `max(seq)` returns `{"items":[]}` (HTTP 200), not 404. Clients can re-poll without state-resync. 404 is reserved for "the session was deleted mid-loop" — that's the signal to stop polling.

**No filters.** No `event_type` / `since` / `until` on this endpoint — filter misses would scan repeatedly without advancing the cursor. Use `GET /events` for analytical queries.

```jsonc
// GET /sessions/sess_a/events?since_seq=42
{
  "items": [
    { "id": "evt_x", "session_id": "sess_a", "seq": 43, "ts": "...",
      "event_type": "tool_call_result", "duration_ms": 50, "payload": {…} },
    { "id": "evt_y", "session_id": "sess_a", "seq": 44, "ts": "...",
      "event_type": "llm_response", "duration_ms": 312, "payload": {…} }
  ]
}
```

### Response shape

```jsonc
// GET /sessions
{
  "items": [
    {
      "id": "...",
      "entity_id": "user_1",
      "group_id": "group_x",
      "title": "Reset forgotten password",  // omitted until opentalon-core's title-generation pass populates it (typically after the first assistant turn)
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

`/events/stats` returns just the totals block by default.

#### Assistant `tool_calls` on `/sessions/{id}`

Each assistant row in `messages[]` carries an optional `tool_calls` field — the raw provider-shaped tool-call array (`id`, `type`, `function.name`, `function.arguments`) lifted verbatim from the matching `llm_response` event's `payload.native_tool_calls_raw`. The field is omitted on rows that did not invoke tools, so user/tool rows and text-only assistant rows stay byte-identical to the pre-passthrough shape.

Pairing is by ordinal: the n-th assistant message receives the n-th `llm_response` event's tool calls (the orchestrator's 1:1 chronological contract). Tool-calling assistant turns therefore render as a single message row with `content` (may be empty) plus `tool_calls`, rather than forcing the consumer to stitch the call back out of the event stream.

```jsonc
// GET /sessions/{id} — assistant turn that emitted tool calls only
{
  "seq": 2,
  "role": "assistant",
  "content": "",
  "tool_calls": [
    { "id": "call_1", "type": "function",
      "function": { "name": "list-items", "arguments": "{}" } }
  ],
  "created_at": "..."
}
```

#### Per-message `metadata` on `/sessions/{id}`

Each row in `messages[]` carries an optional `metadata` field — the raw JSON map stored in the `messages.metadata` column (opentalon-core migration 013), inlined verbatim. It holds UI markers a chat client uses to rebuild the transcript's interactive state after a reload, e.g. a tool-confirmation prompt (`prompt_type: "tool_confirmation"` plus the `tool_call_id` or `pipeline_id` it belongs to) and the user's resolving reply (`prompt_type: "confirmation_response"`, `action: "approve"|"reject"`). The field is omitted when the column is NULL (every ordinary chat turn), so those rows stay byte-identical to the pre-013 shape; a row whose column holds non-JSON is dropped rather than emitted.

```jsonc
// GET /sessions/{id} — assistant turn asking for tool-call confirmation
{
  "seq": 2,
  "role": "assistant",
  "content": "Proceed with deleting 3 items?",
  "metadata": {
    "prompt_type": "tool_confirmation",
    "tool_call_id": "call_9",
    "options": "approve,reject"
  },
  "created_at": "..."
}
```

#### Optional grouping on `/events/stats`

To answer "what's going wrong in conversations" in one round-trip instead of N parallel calls, `/events/stats` accepts two optional params:

- `group_by=event_type` — adds a `by_event_type` array, one bucket per distinct `event_type` in the filtered window. Buckets are ordered `count DESC, event_type ASC` (deterministic). Default response shape (without the param) is unchanged.
- `sample_sessions=N` (1–5) — only valid combined with `group_by=event_type`; each bucket then gains `sample_session_ids`: up to N distinct session IDs that contain at least one event of that type in the window, ordered by most-recent-first per session.

```jsonc
// GET /events/stats?group_by=event_type&sample_sessions=3
{
  "session_count": 142,
  "event_count": 1934,
  "llm_call_count": 631,
  "tool_call_count": 412,
  "tokens_in_total": ...,
  "tokens_out_total": ...,
  "cost_input_total": ...,
  "cost_output_total": ...,
  "by_event_type": [
    { "event_type": "llm_response",          "count": 631, "sample_session_ids": ["sess_a1","sess_b2","sess_c3"] },
    { "event_type": "tool_call_result",      "count": 412, "sample_session_ids": ["sess_a1","sess_b2","sess_c3"] },
    { "event_type": "retry",                 "count": 156, "sample_session_ids": ["sess_d4","sess_e5","sess_f6"] },
    { "event_type": "tool_call_not_found",   "count":  89, "sample_session_ids": ["sess_g7","sess_h8","sess_i9"] }
  ]
}
```

When `group_by=event_type` is set but the filtered window contains no events, `by_event_type` is omitted from the response (top-level counters report zero).

#### Time-series buckets on `/events/stats`

For charting cost / token / call-count curves over time without paging through `/sessions` and bucketing client-side, `/events/stats` accepts `bucket_by`:

- `bucket_by=day|week|month|year` — adds a `time_buckets[]` array, one bucket per period in `[since, until)`. Default response shape (without the param) is unchanged.
- `since` **and** `until` are required when `bucket_by` is set (the time-window bounds bound the result set). Without both, the request is rejected with 400.
- `bucket_by` cannot be combined with `group_by` in v1 (cross-tab `time × event_type` is deferred until a real consumer needs it).

Bucket-count caps (DoS protection, generous on purpose):

| `bucket_by` | Cap | Approximate window |
|---|---|---|
| `day`   | 1200 | ≈ 3.3 years |
| `week`  |  520 | 10 years |
| `month` |  120 | 10 years |
| `year`  |   20 | 20 years |

Exceeding the cap returns 400.

```jsonc
// GET /events/stats?bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z&entity_id=u_alice
{
  "session_count": 1, "event_count": 2, "llm_call_count": 2, "tool_call_count": 0,
  "tokens_in_total": 300, "tokens_out_total": 150,
  "cost_input_total": 0.030, "cost_output_total": 0.060,
  "time_buckets": [
    {"bucket":"2026-05-10","session_count":1,"event_count":1,"llm_call_count":1,"tool_call_count":0,"tokens_in_total":100,"tokens_out_total":50,"cost_input_total":0.010,"cost_output_total":0.020},
    {"bucket":"2026-05-11","session_count":0,"event_count":0,"llm_call_count":0,"tool_call_count":0,"tokens_in_total":0,"tokens_out_total":0,"cost_input_total":0,"cost_output_total":0},
    {"bucket":"2026-05-12","session_count":1,"event_count":1,"llm_call_count":1,"tool_call_count":0,"tokens_in_total":200,"tokens_out_total":100,"cost_input_total":0.020,"cost_output_total":0.040},
    {"bucket":"2026-05-13","session_count":0, …},
    …
  ]
}
```

**Bucket-key format.** Always `YYYY-MM-DD`, anchored to the bucket start: day = that day, week = Monday of the week (ISO 8601), month = 1st of the month, year = January 1. The format is uniform across granularities so a chart-side date parser handles every response.

**Empty bucket fill.** Periods inside `[since, until)` that contain zero events return a bucket with all-zero counters — the chart x-axis is continuous without a client-side fill loop. Always-present scaffold, even for fully empty windows.

**Edge buckets may be partial.** When `since` / `until` don't align with a bucket boundary (e.g. `bucket_by=month&since=2026-05-13&until=2026-06-10` produces buckets `2026-05-01` and `2026-06-01` where both contain only partial-month data), the bucket-key reflects the natural period start but counters include only events within `[since, until)`. If you need only full buckets, align your window to the granularity yourself.

**Sum-consistency.** For every counter except `session_count`, Σ(`time_buckets[].counter`) == top-level counter. The exception is by design: a session active in more than one bucket period contributes 1 to each bucket's `session_count` but is counted once at top level (distinct sessions). Long-running sessions therefore make Σ(buckets) ≥ top-level for that one field — without it, the per-bucket session count would mis-represent "who was active this day".

**Filter axis.** `since` / `until` always apply to the event timestamp (`session_events.ts`) — see the shared filters block above. This is the same axis used by `/sessions` rows, `/events` rows, `/events/stats` totals, and `/events/stats` time buckets, so Σ(buckets) matches top-level and the same window selects the same row set everywhere.

**Performance hint.** Scope queries with `entity_id`, `include_entity_ids`, or `group_id` for fastest response. Unscoped windows (admin-only "all tenants, multi-year") scan the full event stream — they work but are noticeably slower; the indexing improvement that closes this gap lives in opentalon-core.

**Single aggregate over an arbitrary window.** Omit `bucket_by` entirely — the top-level fields are the single aggregate for `[since, until)`. There is no `bucket_by=all`; the no-param call already serves it.

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

# Cross-session cost rollup for a group, excluding internal/staff actors
# so list rows and totals stay coherent for a tenant-facing cost view
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events/stats?group_id=team-a&since=2024-01-01T00:00:00Z&exclude_entity_ids=staff_42,staff_43'

# Event-type breakdown + drill-down sample IDs in one round-trip
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events/stats?group_id=team-a&group_by=event_type&sample_sessions=3'

# Daily cost curve for a tenant over the last 30 days (time-series chart)
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events/stats?group_id=team-a&bucket_by=day&since=2026-04-15T00:00:00Z&until=2026-05-15T00:00:00Z'

# All llm_response events in a session, payload-light
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/events?session_id=sess_a&event_type=llm_response&include_payload=false'

# Tail-poll a session's events incrementally (resume after seq=42)
curl -s -H "Authorization: Bearer your-secret-token" \
  'https://opentalon.example.com/api/sessions/sess_a/events?since_seq=42'
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
