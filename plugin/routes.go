// Package plugin implements the api-plugin HTTP surface — a read-only
// REST API over OpenTalon's sessions, session_events, and prompt_snapshots
// tables. Routes are intentionally narrow: the Rails review UI is the
// primary consumer, and the upstream contract is "five endpoints, JIT
// SQL aggregation, no denormalised counters". See README.md for the
// per-endpoint shape and the design memo for the rationale.
package plugin

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// routes registers all HTTP handlers.
//
// Five data endpoints plus /health, one purpose each — anything not on
// this list is the review UI's job (filter UX, CSV export, locale
// formatting). Adding a sixth data endpoint means re-litigating the
// "JIT SQL is the boundary" memo.
//
// Envelope shapes intentionally differ across the three list/aggregate
// endpoints:
//   - /sessions returns {items, totals, next_cursor} — the review UI
//     surfaces "monthly cost €X across N sessions" above a paginated
//     table, so the totals ride along to save a round-trip.
//   - /events returns {items, next_cursor} — events lists are an
//     analytics affordance; consumers that want aggregates call
//     /events/stats explicitly.
//   - /events/stats returns a bare EventStats object — no list, just
//     the cross-session rollup.
func (h *Handler) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /sessions", h.handleListSessions)
	mux.HandleFunc("GET /sessions/{id}", h.handleGetSession)
	// The one mutating route: rename a session (title only). Writes through the
	// dedicated read-write pool; see handleUpdateSessionTitle.
	mux.HandleFunc("PATCH /sessions/{id}", h.handleUpdateSessionTitle)
	mux.HandleFunc("GET /sessions/{id}/events", h.handleSessionEvents)
	// GET /sessions/{id}/debug-events is the 6th data endpoint, a
	// deliberate exception to the "five endpoints" cap above: the
	// structured session_events log records *that* an LLM call happened
	// and its costs, but never the raw HTTP request/response bytes sent
	// to and returned by the provider. Diagnosing a bad answer (wrong
	// tool schema sent, provider error body, truncated streaming
	// response) requires the verbatim bodies from ai_debug_events, which
	// no other endpoint exposes — raw-body inspection is the one job the
	// review UI cannot do with the existing surface, so it earns its own
	// route rather than overloading /sessions/{id}/events.
	mux.HandleFunc("GET /sessions/{id}/debug-events", h.handleSessionDebugEvents)
	mux.HandleFunc("GET /events", h.handleListEvents)
	mux.HandleFunc("GET /events/stats", h.handleEventsStats)
	mux.HandleFunc("GET /prompt-snapshots", h.handlePromptSnapshot)
	return mux
}

// --- JSON response types ---

// SessionStats are the JIT-aggregated counters/totals for one session.
// Populated by a LEFT JOIN onto session_events in the same query that
// reads the session row — no denormalised counters on the sessions table,
// no aggregator worker. Switching to a rollup table is the documented
// escalation path if /sessions latency exceeds the 200 ms budget; until
// then we pay the query cost on read and keep the schema flat.
type SessionStats struct {
	LLMCallCount    int     `json:"llm_call_count"`
	ToolCallCount   int     `json:"tool_call_count"`
	TokensInTotal   int64   `json:"tokens_in_total"`
	TokensOutTotal  int64   `json:"tokens_out_total"`
	CostInputTotal  float64 `json:"cost_input_total"`
	CostOutputTotal float64 `json:"cost_output_total"`
}

// SessionListItem is one row of GET /sessions. Title is the short LLM-
// generated session label populated by the orchestrator's background
// title-generation pass after the first assistant turn; absent (NULL in
// the sessions row) for pre-generation turns and for sessions written
// before the column existed, so consumers must treat empty/missing as
// "no title yet" rather than an error.
type SessionListItem struct {
	ID          string       `json:"id"`
	EntityID    string       `json:"entity_id,omitempty"`
	GroupID     string       `json:"group_id,omitempty"`
	Title       string       `json:"title,omitempty"`
	Summary     string       `json:"summary,omitempty"`
	ActiveModel string       `json:"active_model,omitempty"`
	CreatedAt   string       `json:"created_at"`
	UpdatedAt   string       `json:"updated_at"`
	Stats       SessionStats `json:"stats"`
}

// SessionListResponse is the GET /sessions envelope. Totals are computed
// over the FULL filtered set (not just the current page) so the UI can
// show "monthly cost €X across N sessions" alongside the paginated rows
// without a second round-trip.
type SessionListResponse struct {
	Items      []SessionListItem `json:"items"`
	Totals     SessionTotals     `json:"totals"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

// SessionTotals aggregates over the entire filtered session set.
type SessionTotals struct {
	SessionCount    int     `json:"session_count"`
	LLMCallCount    int64   `json:"llm_call_count"`
	ToolCallCount   int64   `json:"tool_call_count"`
	TokensInTotal   int64   `json:"tokens_in_total"`
	TokensOutTotal  int64   `json:"tokens_out_total"`
	CostInputTotal  float64 `json:"cost_input_total"`
	CostOutputTotal float64 `json:"cost_output_total"`
}

// SessionDetail is GET /sessions/{id} — the per-session view. Carries
// the rendered chat (messages) and the structured event log (events)
// side by side. Both are bounded to the requested session so the response
// stays linear in the session's own length.
type SessionDetail struct {
	ID          string            `json:"id"`
	EntityID    string            `json:"entity_id,omitempty"`
	GroupID     string            `json:"group_id,omitempty"`
	Title       string            `json:"title,omitempty"` // see SessionListItem.Title
	Summary     string            `json:"summary,omitempty"`
	ActiveModel string            `json:"active_model,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   string            `json:"created_at"`
	UpdatedAt   string            `json:"updated_at"`
	Stats       SessionStats      `json:"stats"`
	Messages    []Message         `json:"messages"`
	Events      []Event           `json:"events"`
}

// Message is one row of the messages table — the user/assistant transcript.
//
// ToolCalls is a passthrough of the matching llm_response event's
// payload.native_tool_calls_raw — the raw provider-shaped tool-call
// array (id, type, function.name, function.arguments). It is populated
// only on assistant rows that actually invoked tools and is omitted
// otherwise, so user/tool rows (and assistant rows that emitted only
// text) stay byte-identical to the pre-passthrough contract.
//
// The messages table itself does not store tool_calls; the data lives in
// session_events.payload, and the serializer pairs the n-th assistant
// message with the n-th llm_response event in chronological order — the
// orchestrator's 1:1 contract. See annotateAssistantToolCalls.
//
// Metadata is the per-message metadata column (opentalon-core migration 013):
// a small JSON map of UI markers (e.g. a tool-confirmation prompt's
// prompt_type + tool_call_id/pipeline_id, or a reply's confirmation_response +
// action) that a chat client uses to rebuild the confirmation UI after a
// reload. It is inlined raw and omitted when the column is NULL, so rows
// without metadata stay byte-identical to the pre-013 contract.
type Message struct {
	Seq       int             `json:"seq"`
	Role      string          `json:"role"`
	Content   string          `json:"content"`
	ToolCalls json.RawMessage `json:"tool_calls,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt string          `json:"created_at"`
}

// Event is one row of session_events. Payload is inlined as a raw JSON
// object so consumers don't double-unmarshal; include_payload=false on
// /events nils it out for byte-efficient analytics calls.
//
// Note: payload size is not capped server-side — a misconfigured
// upstream writer could produce multi-MB rows. The expected mitigation
// is the consumer opting into include_payload=false when paging analytics
// (the Rails review UI does this for its dashboard tile); a hard cap
// belongs on the writer side in opentalon, not here.
type Event struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"session_id"`
	Seq        int             `json:"seq"`
	TS         string          `json:"ts"`
	EventType  string          `json:"event_type"`
	ParentID   string          `json:"parent_id,omitempty"`
	DurationMS int64           `json:"duration_ms,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// EventListResponse is the GET /events envelope. No totals here on
// purpose — /events/stats is the explicit aggregation endpoint, keeping
// the list path linear and the stats path reusable from places that
// don't need the row list at all.
type EventListResponse struct {
	Items      []Event `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

// SessionEventsResponse is the GET /sessions/{id}/events envelope. No
// next_cursor field: `seq` itself is the cursor — the client passes
// max(items[].seq) as the next ?since_seq. A separate type (vs reusing
// EventListResponse with NextCursor + omitempty) keeps the contract
// unambiguous about cursor presence.
type SessionEventsResponse struct {
	Items []Event `json:"items"`
}

// DebugEvent is one raw LLM request/response/error row from
// ai_debug_events — the verbatim HTTP traffic between OpenTalon and the
// provider, surfaced by GET /sessions/{id}/debug-events.
//
// Body is a plain string, NOT json.RawMessage: request and response rows
// hold valid provider JSON, but direction='error' rows store a
// "Class: message" diagnostic that is not valid JSON. Typing Body as
// json.RawMessage would make the encoder emit those error rows as
// malformed JSON. The Rails side parses Body itself only when it needs
// the provider_response_id correlation (response rows), so a string is
// both safe and sufficient.
type DebugEvent struct {
	ID        string `json:"id"`
	TraceID   string `json:"trace_id"`
	TS        string `json:"ts"`
	Direction string `json:"direction"`
	Status    int    `json:"status"`
	URL       string `json:"url"`
	Body      string `json:"body"`
}

// DebugEventsResponse is the GET /sessions/{id}/debug-events envelope,
// mirroring SessionEventsResponse: a bare {items} list, no cursor —
// debug rows are read as a whole bounded session replay, not tail-polled.
type DebugEventsResponse struct {
	Items []DebugEvent `json:"items"`
}

// EventStats is GET /events/stats — cross-session aggregates that mirror
// SessionTotals plus the session count, scoped by the same filters.
//
// ByEventType is populated only when ?group_by=event_type is set AND the
// filtered window contains events. Each bucket's SampleSessionIDs is
// populated only when ?sample_sessions=N (1..5) is also set. The
// omit-when-empty shape keeps the default response identical to the
// pre-group_by contract — and, by the same JSON semantics, drops
// by_event_type from the response when group_by was requested over an
// empty window (the consumer knows from their own request).
type EventStats struct {
	SessionCount    int               `json:"session_count"`
	EventCount      int64             `json:"event_count"`
	LLMCallCount    int64             `json:"llm_call_count"`
	ToolCallCount   int64             `json:"tool_call_count"`
	TokensInTotal   int64             `json:"tokens_in_total"`
	TokensOutTotal  int64             `json:"tokens_out_total"`
	CostInputTotal  float64           `json:"cost_input_total"`
	CostOutputTotal float64           `json:"cost_output_total"`
	ByEventType     []EventTypeBucket `json:"by_event_type,omitempty"`
	TimeBuckets     []TimeBucket      `json:"time_buckets,omitempty"`
}

// TimeBucket is one row of the time_buckets[] breakdown, populated only
// when ?bucket_by=day|week|month|year is set. Bucket is a YYYY-MM-DD
// date string anchored to the bucket start (Monday for week, 1st for
// month, Jan 1 for year). All counters are zero when the bucket falls
// inside [since,until) but contains no events — empty days/weeks/etc.
// are filled server-side so the consumer can render a continuous x-axis
// without a "fill missing periods" loop.
//
// Sum-consistency: Σ(time_buckets[].cost_*) == top-level cost_*; same
// for event_count and the call/token counters. SessionCount is the only
// exception — a session that has events on multiple bucket days is
// counted once per bucket it touches, but only once at top level
// (distinct sessions). Σ(time_buckets[].session_count) ≥ top-level
// session_count therefore, by design — long-running sessions
// contribute to each bucket they're active in.
type TimeBucket struct {
	Bucket          string  `json:"bucket"`
	SessionCount    int     `json:"session_count"`
	EventCount      int64   `json:"event_count"`
	LLMCallCount    int64   `json:"llm_call_count"`
	ToolCallCount   int64   `json:"tool_call_count"`
	TokensInTotal   int64   `json:"tokens_in_total"`
	TokensOutTotal  int64   `json:"tokens_out_total"`
	CostInputTotal  float64 `json:"cost_input_total"`
	CostOutputTotal float64 `json:"cost_output_total"`
}

// EventTypeBucket is one row of the by_event_type breakdown. SampleSessionIDs
// is omitted from the JSON unless ?sample_sessions=N is set AND at least one
// session matches the bucket — same omit-when-empty semantics as
// EventStats.ByEventType, so the two fields stay symmetric. Ordering of
// buckets in EventStats.ByEventType is (count DESC, event_type ASC) —
// deterministic so the consumer can concatenate / diff result sets across
// windows.
type EventTypeBucket struct {
	EventType        string   `json:"event_type"`
	Count            int64    `json:"count"`
	SampleSessionIDs []string `json:"sample_session_ids,omitempty"`
}

// PromptSnapshot is GET /prompt-snapshots?sha=... — the read side of the
// content-addressed prompt store, used by the review UI when a turn_start
// event references a system prompt or tool description by sha256.
type PromptSnapshot struct {
	SHA256    string `json:"sha256"`
	Kind      string `json:"kind"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

// --- Helpers ---

// defaultLimit / maxLimit cap the page size. Defaults err small because
// the review UI lists are 25 rows tall; the cap exists so a misconfigured
// consumer can't ask for a million rows in one round-trip.
const (
	defaultLimit = 25
	maxLimit     = 200
)

// debugDefaultLimit / debugMaxLimit cap GET /sessions/{id}/debug-events.
// Larger than the analytics defaults (see debugLimitFromQuery): raw-body
// replay pulls a whole short session at once, but the 500 cap still stops
// a runaway pull of multi-MB bodies.
const (
	debugDefaultLimit = 200
	debugMaxLimit     = 500
)

// maxSampleSessions caps ?sample_sessions=N on /events/stats. 5 covers
// the "give me a couple of examples to seed a drill-down" use case while
// bounding the per-bucket secondary query and the response size — at
// the documented ~24-event-type ceiling and 5 IDs per bucket the upper
// bound is 120 small ID strings.
const maxSampleSessions = 5

// Granularity values for ?bucket_by= on /events/stats. The set is closed
// in v1; sub-day granularities (hour/minute) and "all" are intentionally
// omitted — "single aggregate" is what /events/stats without bucket_by
// already returns, and hour/minute have a different cap profile so they
// belong in their own iteration.
const (
	bucketByDay   = "day"
	bucketByWeek  = "week"
	bucketByMonth = "month"
	bucketByYear  = "year"
)

// Per-granularity bucket-count caps. Generous on purpose: the cap is
// DoS protection (refuse "year-of-empty-days" pathological queries),
// not a design statement about how far back consumers should chart.
// Real response size at the cap is bounded — 1200 daily buckets ≈
// ~220KB JSON / ~25KB gzipped, fine for an analytical endpoint.
const (
	maxBucketCountDay   = 1200 // ≈ 3.3 years
	maxBucketCountWeek  = 520  // ≈ 10 years
	maxBucketCountMonth = 120  // = 10 years
	maxBucketCountYear  = 20   // = 20 years
)

// maxBucketCountFor returns the per-granularity cap. Granularity is
// validated upstream; an unknown value here is a programmer error and
// returns 0 (which then fails the "exceeds" check trivially).
func maxBucketCountFor(granularity string) int {
	switch granularity {
	case bucketByDay:
		return maxBucketCountDay
	case bucketByWeek:
		return maxBucketCountWeek
	case bucketByMonth:
		return maxBucketCountMonth
	case bucketByYear:
		return maxBucketCountYear
	}
	return 0
}

// truncateToBucket UTC-truncates t to the start of the bucket it falls
// in. Mirrors the SQL DateBucket helper so the Go-side empty-bucket
// fill iterator generates the exact same keys the SQL groups produce.
func truncateToBucket(t time.Time, granularity string) time.Time {
	t = t.UTC()
	switch granularity {
	case bucketByDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case bucketByWeek:
		// ISO 8601: Monday-anchored. time.Weekday: Sunday=0..Saturday=6.
		offset := (int(t.Weekday()) + 6) % 7 // Mon→0, Tue→1, …, Sun→6
		return time.Date(t.Year(), t.Month(), t.Day()-offset, 0, 0, 0, 0, time.UTC)
	case bucketByMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	case bucketByYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return t
}

// advanceBucket steps t by exactly one bucket. Used by the empty-bucket
// fill iterator and by the bucket-count validator. Year/month use
// AddDate so DST and month-length variability are handled by stdlib.
func advanceBucket(t time.Time, granularity string) time.Time {
	switch granularity {
	case bucketByDay:
		return t.AddDate(0, 0, 1)
	case bucketByWeek:
		return t.AddDate(0, 0, 7)
	case bucketByMonth:
		return t.AddDate(0, 1, 0)
	case bucketByYear:
		return t.AddDate(1, 0, 0)
	}
	return t
}

// bucketCountExceeds reports whether the number of buckets fully or
// partially covered by [since, until) exceeds cap. Walks the bucket
// sequence so it stays correct under month-length / leap-year drift
// without re-deriving the bucket math per granularity.
func bucketCountExceeds(since, until time.Time, granularity string, cap int) bool {
	n := 0
	for cur := truncateToBucket(since, granularity); cur.Before(until); cur = advanceBucket(cur, granularity) {
		n++
		if n > cap {
			return true
		}
	}
	return false
}

// Event-type constants mirror events.Type* in opentalon. Keeping the
// strings local (rather than importing the opentalon Go package just to
// reference them) avoids coupling api-plugin's release cadence to
// opentalon's — if a new event type is added upstream we read it
// through anyway, we just don't pre-classify it here.
const (
	eventTypeLLMResponse    = "llm_response"
	eventTypeLLMError       = "llm_error"
	eventTypeToolCallResult = "tool_call_result"
)

// Payload field names — must match the JSON tags on
// events.LLMResponsePayload in opentalon. Centralised so a typo can't
// drift between the four queries that extract from llm_response payloads.
const (
	payloadFieldTokensIn   = "tokens_in"
	payloadFieldTokensOut  = "tokens_out"
	payloadFieldCostInput  = "cost_input"
	payloadFieldCostOutput = "cost_output"
)

// limitFromQuery reads ?limit= with defaults + cap.
//
// Empty value → defaultLimit. Anything outside (1..maxLimit] → 400.
// "Out of bounds → 400" matches every other cap in the API (entity ID
// lists, sample_sessions, bucket counts). A consumer wanting more rows
// than maxLimit should use cursor pagination — silent clamping would
// hide the constraint and surprise the caller who expected v rows back.
func limitFromQuery(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("limit: must be a positive integer, got %q", raw)
	}
	if v > maxLimit {
		return 0, fmt.Errorf("limit: at most %d, got %d", maxLimit, v)
	}
	return v, nil
}

// debugLimitFromQuery reads `?limit=` for the debug-events endpoint.
// It deliberately does NOT reuse limitFromQuery: raw-body replay wants a
// larger default (200, a whole short session in one shot) and a higher
// cap (500) than the analytics list endpoints' 25/200, since the consumer
// is a one-off "show me the full exchange" inspection, not a paged feed.
func debugLimitFromQuery(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return debugDefaultLimit, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("limit: must be a positive integer, got %q", raw)
	}
	if v > debugMaxLimit {
		return 0, fmt.Errorf("limit: at most %d, got %d", debugMaxLimit, v)
	}
	return v, nil
}

// sinceSeqFromQuery reads the optional `?since_seq=` cursor on the
// tail-poll endpoint. Empty → 0 (full session). Negative values are
// rejected: seq starts at 1, so `seq > -N` would match every row in
// the session on every poll — quietly turning the tail-poll into a
// full re-fetch.
func sinceSeqFromQuery(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("since_seq")
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("since_seq: must be an integer, got %q", raw)
	}
	if v < 0 {
		return 0, fmt.Errorf("since_seq: must be non-negative, got %d", v)
	}
	return v, nil
}

// boolFromQuery reads an optional bool query param ("true"/"false"
// case-sensitive). Empty returns def. Anything else is 400 — silent
// fallback would let `?include_payload=banana` pass as the default,
// hiding a consumer typo.
func boolFromQuery(r *http.Request, key string, def bool) (bool, error) {
	raw := r.URL.Query().Get(key)
	switch raw {
	case "":
		return def, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s: must be \"true\" or \"false\", got %q", key, raw)
	}
}

// cursorPair is the (sort-key-value, id) anchor for cursor pagination on
// /events. /sessions uses sessionCursor instead, which additionally
// records the active sort identifier — see the comment on that type.
//
// Why a composite cursor: row IDs aren't k-sortable (opentalon generates
// them per-actor), so "<key> < ?" alone is non-unique across concurrent
// inserts. The composite WHERE clause:
//
//	<key> < ? OR (<key> = ? AND id < ?)
//
// gives a total order that survives reseeds and duplicate keys. Encoding
// is base64url over "<value>|<id>" — opaque to clients (they copy
// next_cursor blindly) so we can change the shape later without breaking
// the contract.
type cursorPair struct {
	TS string
	ID string
}

func encodeCursor(c cursorPair) string {
	if c.TS == "" && c.ID == "" {
		return ""
	}
	raw := c.TS + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (cursorPair, error) {
	if s == "" {
		return cursorPair{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursorPair{}, fmt.Errorf("cursor: invalid encoding")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return cursorPair{}, fmt.Errorf("cursor: malformed")
	}
	return cursorPair{TS: parts[0], ID: parts[1]}, nil
}

// sessionCursor is the per-page anchor for /sessions. Unlike cursorPair
// it carries the active sort identifier (key + direction) alongside the
// boundary value, so a cursor minted under `sort=cost_total&direction=desc`
// is rejected with 400 when replayed against a different sort context —
// "the cursor would walk a meaningless keyset" is a documented failure
// mode of switching sort mid-pagination.
//
// Wire format is base64url over "<sort>|<direction>|<value>|<id>" (four
// fields). For backward compat with cursors minted before sort was
// configurable, the decoder also accepts the legacy two-field
// "<value>|<id>" shape and interprets it as the default sort
// (created_at, desc) so in-flight cursors survive deploys.
type sessionCursor struct {
	Sort  sessionSort
	Value string
	ID    string
}

func encodeSessionCursor(c sessionCursor) string {
	if c.Value == "" && c.ID == "" {
		return ""
	}
	raw := c.Sort.Key + "|" + c.Sort.Direction + "|" + c.Value + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeSessionCursor(s string) (sessionCursor, error) {
	if s == "" {
		return sessionCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return sessionCursor{}, fmt.Errorf("cursor: invalid encoding")
	}
	// SplitN cap is 4 so a session ID that happens to contain "|" still
	// round-trips cleanly — the trailing piece absorbs the remainder rather
	// than getting rejected as malformed. Mirrors the SplitN(",",2) the
	// legacy /events decoder uses on cursorPair for the same reason.
	parts := strings.SplitN(string(raw), "|", 4)
	switch len(parts) {
	case 2:
		// Pre-sort cursor: created_at DESC implicit.
		return sessionCursor{Sort: defaultSessionSort, Value: parts[0], ID: parts[1]}, nil
	case 4:
		return sessionCursor{
			Sort:  sessionSort{Key: parts[0], Direction: parts[1]},
			Value: parts[2],
			ID:    parts[3],
		}, nil
	default:
		return sessionCursor{}, fmt.Errorf("cursor: malformed")
	}
}

// timeRangeFromQuery reads ?since= / ?until= as RFC3339 timestamps. An
// empty value returns ("", nil) so callers can treat "no filter" and
// "valid filter" with the same code path; an unparseable value returns
// an error so the handler can 400 instead of silently dropping it.
func timeRangeFromQuery(r *http.Request) (since, until string, err error) {
	since = r.URL.Query().Get("since")
	until = r.URL.Query().Get("until")
	for _, v := range []string{since, until} {
		if v == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return "", "", fmt.Errorf("invalid timestamp %q: expected RFC3339", v)
		}
	}
	return since, until, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// writeServerErr is the 5xx counterpart of writeErr: logs the real error
// server-side (with route context for ops) and returns a generic message
// to the client so SQL details, file paths, dialect quirks, and so on
// don't leak past the trust boundary. Use this anywhere a non-client
// error needs to become a 500.
func writeServerErr(w http.ResponseWriter, r *http.Request, route string, err error) {
	log.Printf("api-plugin: 500 on %s %s: %v", r.Method, route, err)
	writeErr(w, http.StatusInternalServerError, "internal server error")
}

// --- HTTP Handlers ---

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if err := h.db.Ping(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "db unreachable")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// sessionFilters captures the shared filter set across /sessions and
// /events/stats. Centralised so a new filter (e.g. session_id pattern)
// gets added in one place and reaches both endpoints.
type sessionFilters struct {
	EntityID         string
	GroupID          string
	IncludeEntityIDs []string // entity_ids to include (multi-actor scope); ANDed with singular EntityID
	ExcludeEntityIDs []string // entity_ids to exclude from rows AND aggregation
	Since            string   // RFC3339, empty = no lower bound
	Until            string   // RFC3339, empty = no upper bound
	TitleQuery       string   // case-insensitive substring match on title; empty = no title filter
}

func filtersFromQuery(r *http.Request) (sessionFilters, error) {
	since, until, err := timeRangeFromQuery(r)
	if err != nil {
		return sessionFilters{}, err
	}
	includeIDs, err := parseCommaList(r.URL.Query().Get("include_entity_ids"), maxEntityIDList)
	if err != nil {
		return sessionFilters{}, fmt.Errorf("include_entity_ids: %w", err)
	}
	excludeIDs, err := parseCommaList(r.URL.Query().Get("exclude_entity_ids"), maxEntityIDList)
	if err != nil {
		return sessionFilters{}, fmt.Errorf("exclude_entity_ids: %w", err)
	}
	return sessionFilters{
		EntityID:         r.URL.Query().Get("entity_id"),
		GroupID:          r.URL.Query().Get("group_id"),
		IncludeEntityIDs: includeIDs,
		ExcludeEntityIDs: excludeIDs,
		Since:            since,
		Until:            until,
	}, nil
}

// maxTitleQuery caps the ?q= title search term. A title is a short label;
// anything longer is not a real search and would only bloat the LIKE pattern.
const maxTitleQuery = 200

// titleQueryFromRequest reads and bounds the ?q= session-title search term.
func titleQueryFromRequest(r *http.Request) string {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if rq := []rune(q); len(rq) > maxTitleQuery {
		q = string(rq[:maxTitleQuery])
	}
	return q
}

// maxEntityIDList caps the include/exclude_entity_ids lists. Realistic
// callers scope to a handful of actors; the cap protects against a
// pathological URL that would (a) hit SQLite's ~999 bind-param ceiling
// and (b) produce an IN/NOT IN with thousands of placeholders.
const maxEntityIDList = 200

// --- /sessions sort ---

// Sort-key constants are duplicated as the public query-string vocabulary,
// so the keys also appear in the README. Add a new key in three places:
// the constant block, sessionSortFromQuery's allow-list, and
// resolveSessionSort's switch.
const (
	sortKeyCreatedAt     = "created_at"
	sortKeyUpdatedAt     = "updated_at"
	sortKeyCostTotal     = "cost_total"
	sortKeyLLMCallCount  = "llm_call_count"
	sortKeyToolCallCount = "tool_call_count"
	sortKeyTokensIn      = "tokens_in_total"
	sortKeyTokensOut     = "tokens_out_total"

	sortDirAsc  = "asc"
	sortDirDesc = "desc"
)

// sessionSort is the parsed sort spec from the query string. Zero value
// (both fields empty) is illegal — sessionSortFromQuery always fills in
// the default before returning.
type sessionSort struct {
	Key       string
	Direction string
}

var defaultSessionSort = sessionSort{Key: sortKeyCreatedAt, Direction: sortDirDesc}

// sessionSortDef is the dialect-resolved form: the SQL expression to
// splice into ORDER BY (and HAVING for aggregate sorts), plus the
// per-row extractor / cursor parser that handle the boundary value's
// Go type round-trip.
type sessionSortDef struct {
	sessionSort
	Expr        string                        // SQL fragment, ready to splice
	IsAggregate bool                          // → cursor predicate lives in HAVING, not WHERE
	Extract     func(SessionListItem) string  // serialize a row's sort value into the cursor string
	BindValue   func(raw string) (any, error) // parse the cursor string back into the typed Go value used as a SQL bind
}

// sessionSortFromQuery validates ?sort= / ?direction= and fills defaults.
// Returns 400-shaped errors for unknown keys so the consumer learns the
// allow-list instead of silently getting a created_at sort back.
func sessionSortFromQuery(r *http.Request) (sessionSort, error) {
	s := r.URL.Query().Get("sort")
	d := r.URL.Query().Get("direction")
	if s == "" {
		s = defaultSessionSort.Key
	}
	if d == "" {
		d = defaultSessionSort.Direction
	}
	switch s {
	case sortKeyCreatedAt, sortKeyUpdatedAt, sortKeyCostTotal, sortKeyLLMCallCount,
		sortKeyToolCallCount, sortKeyTokensIn, sortKeyTokensOut:
	default:
		return sessionSort{}, fmt.Errorf(
			"sort: unsupported key %q (allowed: %s, %s, %s, %s, %s, %s, %s)",
			s, sortKeyCreatedAt, sortKeyUpdatedAt, sortKeyCostTotal, sortKeyLLMCallCount,
			sortKeyToolCallCount, sortKeyTokensIn, sortKeyTokensOut)
	}
	switch d {
	case sortDirAsc, sortDirDesc:
	default:
		return sessionSort{}, fmt.Errorf("direction: must be %q or %q, got %q", sortDirAsc, sortDirDesc, d)
	}
	return sessionSort{Key: s, Direction: d}, nil
}

// resolveSessionSort returns the dialect-resolved sort definition for the
// given (validated) sort spec. The aggregate expressions exactly mirror
// the projections in sessionStatsSelect so the cursor boundary value
// extracted from a row's stats matches what HAVING compares against.
func resolveSessionSort(s sessionSort, d Dialect) sessionSortDef {
	def := sessionSortDef{sessionSort: s}
	switch s.Key {
	case sortKeyCreatedAt:
		def.Expr = "s.created_at"
		def.Extract = func(r SessionListItem) string { return r.CreatedAt }
		def.BindValue = func(raw string) (any, error) { return raw, nil }
	case sortKeyUpdatedAt:
		def.Expr = "s.updated_at"
		def.Extract = func(r SessionListItem) string { return r.UpdatedAt }
		def.BindValue = func(raw string) (any, error) { return raw, nil }
	case sortKeyCostTotal:
		def.Expr = llmAggExpr(d, payloadFieldCostInput, false) + " + " + llmAggExpr(d, payloadFieldCostOutput, false)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string {
			return strconv.FormatFloat(r.Stats.CostInputTotal+r.Stats.CostOutputTotal, 'f', -1, 64)
		}
		def.BindValue = func(raw string) (any, error) { return strconv.ParseFloat(raw, 64) }
	case sortKeyLLMCallCount:
		def.Expr = countEventTypeExpr(eventTypeLLMResponse)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string { return strconv.Itoa(r.Stats.LLMCallCount) }
		def.BindValue = func(raw string) (any, error) { return strconv.ParseInt(raw, 10, 64) }
	case sortKeyToolCallCount:
		def.Expr = countEventTypeExpr(eventTypeToolCallResult)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string { return strconv.Itoa(r.Stats.ToolCallCount) }
		def.BindValue = func(raw string) (any, error) { return strconv.ParseInt(raw, 10, 64) }
	case sortKeyTokensIn:
		def.Expr = llmAggExpr(d, payloadFieldTokensIn, true)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string { return strconv.FormatInt(r.Stats.TokensInTotal, 10) }
		def.BindValue = func(raw string) (any, error) { return strconv.ParseInt(raw, 10, 64) }
	case sortKeyTokensOut:
		def.Expr = llmAggExpr(d, payloadFieldTokensOut, true)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string { return strconv.FormatInt(r.Stats.TokensOutTotal, 10) }
		def.BindValue = func(raw string) (any, error) { return strconv.ParseInt(raw, 10, 64) }
	}
	return def
}

// keysetCmp builds the composite-keyset comparison predicate for cursor
// pagination against `expr`, with `s.id` as the deterministic tiebreaker.
// Caller appends three args in (boundary, boundary, id) order.
//
//	DESC: (expr <  ? OR (expr =  ? AND s.id <  ?))
//	ASC:  (expr >  ? OR (expr =  ? AND s.id >  ?))
func keysetCmp(expr, direction string) string {
	cmp := "<"
	if direction == sortDirAsc {
		cmp = ">"
	}
	return fmt.Sprintf("(%s %s ? OR (%s = ? AND s.id %s ?))", expr, cmp, expr, cmp)
}

// validateCursorMatchesSort 400s on a cursor whose embedded sort doesn't
// match the active request sort. Switching sort mid-pagination would
// otherwise walk a meaningless keyset (the boundary value is in the old
// sort's space, not the new one's) — failing fast surfaces the consumer
// bug instead of returning silently-wrong pages.
func validateCursorMatchesSort(c sessionCursor, want sessionSort) error {
	if c.Value == "" && c.ID == "" {
		return nil
	}
	if c.Sort != want {
		return fmt.Errorf("cursor: sort %s/%s does not match request %s/%s",
			c.Sort.Key, c.Sort.Direction, want.Key, want.Direction)
	}
	return nil
}

// parseCommaList splits a comma-separated query value into trimmed,
// non-empty tokens. Returns nil for an empty/whitespace-only input so
// callers can branch on len(...) without a separate "is set" flag.
// When max > 0 the result is bounded; exceeding the cap is an error so
// the caller can return 400 rather than silently dropping ids the
// requester intended to filter out.
func parseCommaList(raw string, max int) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	if max > 0 && len(out) > max {
		return nil, fmt.Errorf("at most %d values supported, got %d", max, len(out))
	}
	return out, nil
}

// writeInClause appends ` AND <col> NOT IN (?, ?, …)` with one bind per
// id. Centralised so applySessionFilters and listEvents share the exact
// same predicate shape — a future caller (or a third call site) cannot
// drift.
func writeInClause(q *strings.Builder, args *[]any, col string, ids []string, negate bool) {
	if len(ids) == 0 {
		return
	}
	q.WriteString(" AND ")
	q.WriteString(col)
	if negate {
		q.WriteString(" NOT")
	}
	q.WriteString(" IN (")
	for i, id := range ids {
		if i > 0 {
			q.WriteString(",")
		}
		q.WriteString("?")
		*args = append(*args, id)
	}
	q.WriteString(")")
}

func (h *Handler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	f, err := filtersFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Title search is a /sessions-only filter (only sessions carry titles), so it
	// is set here rather than in the shared filtersFromQuery that /events and
	// /events/stats also use. listSessions AND sessionTotals both read f, so the
	// totals stay consistent with the q-filtered rows.
	f.TitleQuery = titleQueryFromRequest(r)

	sort, err := sessionSortFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	cur, err := decodeSessionCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateCursorMatchesSort(cur, sort); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := limitFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	items, next, err := listSessions(h.db, h.dialect, f, sort, cur, limit)
	if err != nil {
		writeServerErr(w, r, "/sessions", err)
		return
	}
	totals, err := sessionTotals(h.db, h.dialect, f)
	if err != nil {
		writeServerErr(w, r, "/sessions", err)
		return
	}
	writeJSON(w, SessionListResponse{
		Items:      items,
		Totals:     totals,
		NextCursor: encodeSessionCursor(next),
	})
}

func (h *Handler) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Staff analytics pass include_hidden=true to see system-injected turns for
	// debugging; the customer chat widget omits it, so hidden turns stay out of
	// the user-facing transcript. Parsed via boolFromQuery so a typo is a 400,
	// not a silent "false" that would quietly show the customer view.
	includeHidden, err := boolFromQuery(r, "include_hidden", false)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sess, err := getSession(h.db, h.dialect, id, includeHidden)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "session not found")
			return
		}
		writeServerErr(w, r, "/sessions/{id}", err)
		return
	}
	writeJSON(w, sess)
}

const (
	// maxTitleLen caps a user-set session title (in runes). Titles are short
	// labels; the column is TEXT but a sane cap stops an oversized write.
	maxTitleLen = 200
	// maxTitleBody caps the PATCH request body so a client can't stream
	// megabytes into the JSON decoder.
	maxTitleBody = 4 << 10 // 4 KiB
)

// handleUpdateSessionTitle renames a session — the api-plugin's single mutating
// endpoint. It writes ONLY the title column, through the dedicated read-write
// pool (h.writeDB); every other endpoint stays on the read-only pool. Ownership
// is enforced upstream (the Rails proxy verifies the session belongs to the
// actor before forwarding); this layer authenticates the bearer token (via
// authMiddleware) and validates the payload. The write is unconditional, so a
// user rename always wins over core's async auto-label — which only fills an
// empty title (see opentalon SessionStore.SetTitle).
func (h *Handler) handleUpdateSessionTitle(w http.ResponseWriter, r *http.Request) {
	// The write pool is opened ONLY when a bearer token is configured (see
	// Configure): a mutating endpoint must never run unauthenticated. Without a
	// token there is no writer, so a rename is refused rather than running open.
	if h.writeDB == nil {
		writeErr(w, http.StatusServiceUnavailable, "session rename is disabled (no API token configured)")
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "session id required")
		return
	}

	var body struct {
		Title string `json:"title"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTitleBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, `invalid JSON body: expected {"title": "..."}`)
		return
	}

	title := strings.TrimSpace(body.Title)
	if title == "" {
		writeErr(w, http.StatusBadRequest, "title must not be empty")
		return
	}
	if len([]rune(title)) > maxTitleLen {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("title too long (max %d characters)", maxTitleLen))
		return
	}

	updatedAt := time.Now().UTC().Format(time.RFC3339)
	res, err := h.writeDB.Exec(
		h.dialect.Rebind(`UPDATE sessions SET title = ?, updated_at = ? WHERE id = ?`),
		title, updatedAt, id)
	if err != nil {
		writeServerErr(w, r, "PATCH /sessions/{id}", err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}

	writeJSON(w, map[string]string{"id": id, "title": title, "updated_at": updatedAt})
}

// handleSessionEvents is the incremental tail-poll endpoint for the
// AI-Sessions diagnostic page. Returns events with seq > since_seq in
// ascending seq order so the client appends directly to its rendered
// log; designed for a 2s polling loop after an initial GET
// /sessions/{id} envelope fetch.
//
// Cursor is `seq` itself — no opaque token, no encode/decode. The
// UNIQUE (session_id, seq) index backs both the per-session monotonic
// gap-free seq guarantee (writer side, retry-on-conflict in
// opentalon-core) and the ORDER BY here on the read side.
//
// 404 is a separate cheap PK probe so empty `items` (caught-up poll)
// is disambiguated from an unknown session id. Sessions are
// append-only in this system, so 404 means "id never existed"
// (typo / probe); polling clients stop. On an empty `items` they keep
// polling at the same cadence.
func (h *Handler) handleSessionEvents(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")

	sinceSeq, err := sinceSeqFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := limitFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	includePayload, err := boolFromQuery(r, "include_payload", true)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	exists, err := sessionExists(h.db, h.dialect, sessionID)
	if err != nil {
		writeServerErr(w, r, "/sessions/{id}/events", err)
		return
	}
	if !exists {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}

	items, err := listSessionEventsAsc(h.db, h.dialect, sessionID, sinceSeq, includePayload, limit)
	if err != nil {
		writeServerErr(w, r, "/sessions/{id}/events", err)
		return
	}
	writeJSON(w, SessionEventsResponse{Items: items})
}

// handleSessionDebugEvents serves GET /sessions/{id}/debug-events: the
// verbatim LLM request/response/error bodies for one session, ordered
// chronologically. Mirrors handleSessionEvents — same path-id read,
// same sessionExists 404 probe (an existing session with no captured
// debug rows returns {"items":[]}, 200) — but uses the debug-specific
// limit (default 200, max 500) since the consumer wants the whole short
// exchange at once, not a paged tail-poll.
func (h *Handler) handleSessionDebugEvents(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")

	limit, err := debugLimitFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	exists, err := sessionExists(h.db, h.dialect, sessionID)
	if err != nil {
		writeServerErr(w, r, "/sessions/{id}/debug-events", err)
		return
	}
	if !exists {
		writeErr(w, http.StatusNotFound, "session not found")
		return
	}

	items, err := listSessionDebugEventsAsc(h.db, h.dialect, sessionID, limit)
	if err != nil {
		writeServerErr(w, r, "/sessions/{id}/debug-events", err)
		return
	}
	writeJSON(w, DebugEventsResponse{Items: items})
}

func (h *Handler) handleListEvents(w http.ResponseWriter, r *http.Request) {
	f, err := filtersFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	cur, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := limitFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	includePayload, err := boolFromQuery(r, "include_payload", true)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	q := r.URL.Query()
	items, next, err := listEvents(h.db, h.dialect, eventListFilters{
		Filters:        f,
		SessionID:      q.Get("session_id"),
		EventType:      q.Get("event_type"),
		IncludePayload: includePayload,
	}, cur, limit)
	if err != nil {
		writeServerErr(w, r, "/events", err)
		return
	}
	writeJSON(w, EventListResponse{
		Items:      items,
		NextCursor: encodeCursor(next),
	})
}

func (h *Handler) handleEventsStats(w http.ResponseWriter, r *http.Request) {
	opts, err := eventsStatsOptsFromQuery(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	stats, err := eventsStats(h.db, h.dialect, opts)
	if err != nil {
		writeServerErr(w, r, "/events/stats", err)
		return
	}
	writeJSON(w, stats)
}

// eventsStatsOptsFromQuery parses the /events/stats query string into the
// opts struct. Mirrors filtersFromQuery — keeps the handler narrow and
// puts all validation in one place where a future param can be added
// without touching the handler.
func eventsStatsOptsFromQuery(r *http.Request) (eventsStatsOpts, error) {
	f, err := filtersFromQuery(r)
	if err != nil {
		return eventsStatsOpts{}, err
	}
	opts := eventsStatsOpts{Filters: f}
	q := r.URL.Query()
	if gb := q.Get("group_by"); gb != "" {
		if gb != "event_type" {
			return eventsStatsOpts{}, fmt.Errorf("group_by: only 'event_type' is supported")
		}
		opts.GroupByEventType = true
	}
	if et := q.Get("event_type"); et != "" {
		// event_type narrows the row set to a single type. Combined with
		// group_by=event_type the breakdown collapses to one bucket —
		// reject as redundant so the consumer notices instead of getting
		// a one-element by_event_type back.
		if opts.GroupByEventType {
			return eventsStatsOpts{}, fmt.Errorf("event_type and group_by=event_type are mutually exclusive (the grouping collapses to one bucket)")
		}
		opts.EventType = et
	}
	if ss := q.Get("sample_sessions"); ss != "" {
		// sample_sessions is meaningless without a grouping dimension —
		// reject explicitly so the consumer notices the mis-call instead
		// of silently getting unsampled buckets back.
		if !opts.GroupByEventType {
			return eventsStatsOpts{}, fmt.Errorf("sample_sessions requires group_by=event_type")
		}
		n, err := strconv.Atoi(ss)
		if err != nil || n < 1 || n > maxSampleSessions {
			return eventsStatsOpts{}, fmt.Errorf("sample_sessions: must be an integer in 1..%d", maxSampleSessions)
		}
		opts.SampleSessions = n
	}
	if bb := q.Get("bucket_by"); bb != "" {
		// Granularity allow-list; case-sensitive on purpose so the URL
		// stays unambiguous and the consumer notices typos.
		switch bb {
		case bucketByDay, bucketByWeek, bucketByMonth, bucketByYear:
		default:
			return eventsStatsOpts{}, fmt.Errorf(
				"bucket_by: unsupported value %q (allowed: %s, %s, %s, %s)",
				bb, bucketByDay, bucketByWeek, bucketByMonth, bucketByYear)
		}
		// bucket_by × group_by=event_type would be a cross-tab
		// (time × event_type matrix). Deferred for v1 — the first
		// consumer doesn't need it; revisit when an actual chart does.
		if opts.GroupByEventType {
			return eventsStatsOpts{}, fmt.Errorf("bucket_by cannot be combined with group_by in v1")
		}
		// since/until are mandatory: without them the bucket count is
		// unbounded ("all events ever, day-by-day"). Better to 400 than
		// to silently scan the full table or trim with a default window.
		if f.Since == "" || f.Until == "" {
			return eventsStatsOpts{}, fmt.Errorf("bucket_by requires both since and until")
		}
		sinceT, _ := time.Parse(time.RFC3339, f.Since) // filtersFromQuery already validated
		untilT, _ := time.Parse(time.RFC3339, f.Until)
		if !sinceT.Before(untilT) {
			return eventsStatsOpts{}, fmt.Errorf("bucket_by requires since < until")
		}
		cap := maxBucketCountFor(bb)
		if bucketCountExceeds(sinceT, untilT, bb, cap) {
			return eventsStatsOpts{}, fmt.Errorf("bucket_by=%s: window exceeds %d buckets (cap)", bb, cap)
		}
		opts.BucketBy = bb
	}
	return opts, nil
}

func (h *Handler) handlePromptSnapshot(w http.ResponseWriter, r *http.Request) {
	sha := r.URL.Query().Get("sha")
	if sha == "" {
		writeErr(w, http.StatusBadRequest, "missing sha parameter")
		return
	}
	snap, err := getPromptSnapshot(h.db, h.dialect, sha)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "prompt snapshot not found")
			return
		}
		writeServerErr(w, r, "/prompt-snapshots", err)
		return
	}
	writeJSON(w, snap)
}

// --- Query functions ---

// countEventTypeExpr returns the SUM-CASE expression for counting events
// of a single type. Used in every JIT aggregation that exposes a per-type
// counter (llm_call_count, tool_call_count). eventType MUST be a hard-
// coded constant from event_types — never user input — because it's
// interpolated rather than bound. Centralised so a CASE/SUM tweak only
// has to happen once.
func countEventTypeExpr(eventType string) string {
	return fmt.Sprintf("SUM(CASE WHEN se.event_type = '%s' THEN 1 ELSE 0 END)", eventType)
}

// llmAggExpr returns the SUM-CASE expression for an llm_response payload
// field. Used in every JIT-aggregation query so the per-event filter is
// consistent.
func llmAggExpr(d Dialect, field string, isInt bool) string {
	var extract string
	if isInt {
		extract = d.JSONInt("se.payload", field)
	} else {
		extract = d.JSONFloat("se.payload", field)
	}
	return fmt.Sprintf("SUM(CASE WHEN se.event_type = '%s' THEN %s ELSE 0 END)",
		eventTypeLLMResponse, extract)
}

// sessionStatsSelect is the per-session aggregation projection shared by
// listSessions and getSession. Returned as a fragment to splice into the
// outer SELECT so each row reads in one query.
func sessionStatsSelect(d Dialect) string {
	return strings.Join([]string{
		fmt.Sprintf("COALESCE(%s, 0) AS llm_call_count", countEventTypeExpr(eventTypeLLMResponse)),
		fmt.Sprintf("COALESCE(%s, 0) AS tool_call_count", countEventTypeExpr(eventTypeToolCallResult)),
		"COALESCE(" + llmAggExpr(d, payloadFieldTokensIn, true) + ", 0) AS tokens_in_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldTokensOut, true) + ", 0) AS tokens_out_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldCostInput, false) + ", 0) AS cost_input_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldCostOutput, false) + ", 0) AS cost_output_total",
	}, ", ")
}

// applySessionFilters appends shared filter predicates and args. The
// sessions table is always aliased "s", session_events always "se".
//
// since/until apply to se.ts (event timestamp), uniform across every
// endpoint that uses this builder. Rationale: "in this time window"
// universally means "events that occurred in the window", not "sessions
// created in the window" — sessions are containers, events are the
// economically-meaningful data points. A session created before the
// window with events inside contributes; a session created inside with
// no events does not. Also makes Σ(time_buckets[].counter) == top-level
// counter hold for every counter on /events/stats without conditional
// time-column gymnastics.
//
// The session_events JOIN must be present in the query whenever this
// helper is called with a non-empty Since/Until (or any other se.* /
// payload-based predicate). All current callers JOIN.
func applySessionFilters(q *strings.Builder, args *[]any, f sessionFilters) {
	if f.EntityID != "" {
		q.WriteString(" AND s.entity_id = ?")
		*args = append(*args, f.EntityID)
	}
	if f.GroupID != "" {
		q.WriteString(" AND s.group_id = ?")
		*args = append(*args, f.GroupID)
	}
	// Both lists hit /sessions (rows + totals) and /events/stats so the
	// caller's row view and aggregates stay coherent — visible rows
	// always sum to the displayed totals. Include and exclude AND
	// together: include narrows the candidate set, exclude removes from
	// what remains. Each is also ANDed with the singular EntityID, so
	// `?entity_id=a&include_entity_ids=a,b` resolves to just `a`.
	writeInClause(q, args, "s.entity_id", f.IncludeEntityIDs, false)
	writeInClause(q, args, "s.entity_id", f.ExcludeEntityIDs, true)
	if f.Since != "" {
		q.WriteString(" AND se.ts >= ?")
		*args = append(*args, f.Since)
	}
	if f.Until != "" {
		q.WriteString(" AND se.ts < ?")
		*args = append(*args, f.Until)
	}
	if f.TitleQuery != "" {
		// Case-insensitive substring match. Both sides are folded by the SAME
		// engine LOWER() — NOT Go's Unicode ToLower — so a title and its search
		// term always agree on the same engine: ASCII-only folding on sqlite,
		// locale-aware folding on a UTF-8 postgres. (Lowering the term in Go
		// instead would silently miss accented titles on the sqlite path.)
		// escapeLike + ESCAPE '\' neutralise %/_ so a term like "50%" matches
		// literally, not as a wildcard; sqlite has no default escape char, hence
		// the explicit clause.
		q.WriteString(` AND LOWER(s.title) LIKE LOWER(?) ESCAPE '\'`)
		*args = append(*args, "%"+escapeLike(f.TitleQuery)+"%")
	}
}

// escapeLike neutralises the LIKE metacharacters (%, _, and the \ escape char)
// in a user search term so it matches literally inside a %…% pattern. Paired
// with an explicit ESCAPE '\' clause.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// applyEventTypeFilter appends an optional event_type predicate. Lives
// outside applySessionFilters because event_type is not a session-level
// filter — only /events and /events/stats use it, /sessions does not
// (a session isn't of "one type", it contains events of many types).
func applyEventTypeFilter(q *strings.Builder, args *[]any, eventType string) {
	if eventType == "" {
		return
	}
	q.WriteString(" AND se.event_type = ?")
	*args = append(*args, eventType)
}

func listSessions(db *sql.DB, d Dialect, f sessionFilters, sort sessionSort, cur sessionCursor, limit int) ([]SessionListItem, sessionCursor, error) {
	def := resolveSessionSort(sort, d)

	var q strings.Builder
	q.WriteString(`SELECT s.id, COALESCE(s.entity_id,''), COALESCE(s.group_id,''),
		COALESCE(s.title,''), COALESCE(s.summary,''), COALESCE(s.active_model,''),
		s.created_at, s.updated_at, `)
	q.WriteString(sessionStatsSelect(d))
	q.WriteString(` FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id WHERE 1=1`)

	var args []any
	applySessionFilters(&q, &args, f)

	// Cursor predicate: per-row sort keys (created_at) compare in WHERE so
	// they prune before GROUP BY; aggregate sort keys (cost_total, token
	// counts, …) must be in HAVING since the value only exists post-aggregation.
	// Argument order stays {filters, cursor, limit} in both branches because
	// HAVING's args follow WHERE's positionally in the rebound SQL.
	var (
		havingCursor    bool
		cursorBindValue any
	)
	if cur.ID != "" {
		v, err := def.BindValue(cur.Value)
		if err != nil {
			return nil, sessionCursor{}, fmt.Errorf("cursor: parse boundary: %w", err)
		}
		cursorBindValue = v
		if def.IsAggregate {
			havingCursor = true
		} else {
			q.WriteString(" AND ")
			q.WriteString(keysetCmp(def.Expr, sort.Direction))
			args = append(args, cursorBindValue, cursorBindValue, cur.ID)
		}
	}

	q.WriteString(` GROUP BY s.id, s.entity_id, s.group_id, s.title, s.summary, s.active_model, s.created_at, s.updated_at`)
	if havingCursor {
		q.WriteString(" HAVING ")
		q.WriteString(keysetCmp(def.Expr, sort.Direction))
		args = append(args, cursorBindValue, cursorBindValue, cur.ID)
	}

	orderDir := "DESC"
	if sort.Direction == sortDirAsc {
		orderDir = "ASC"
	}
	// ORDER BY repeats the expression rather than aliasing to keep both
	// dialects on the same plan; column aliases in ORDER BY are portable
	// but mixing alias-in-ORDER-BY with expression-in-HAVING is asymmetric
	// and a magnet for typo drift between the two sites.
	fmt.Fprintf(&q, ` ORDER BY %s %s, s.id %s LIMIT ?`, def.Expr, orderDir, orderDir)
	// limit+1 trick: probe whether a next page exists without a separate
	// COUNT query. Trim the extra row before returning; if it was there,
	// the last KEPT row supplies the next cursor.
	args = append(args, limit+1)

	rows, err := db.Query(d.Rebind(q.String()), args...)
	if err != nil {
		return nil, sessionCursor{}, fmt.Errorf("listSessions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]SessionListItem, 0, limit)
	for rows.Next() {
		var s SessionListItem
		if err := rows.Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Title, &s.Summary, &s.ActiveModel,
			&s.CreatedAt, &s.UpdatedAt,
			&s.Stats.LLMCallCount, &s.Stats.ToolCallCount,
			&s.Stats.TokensInTotal, &s.Stats.TokensOutTotal,
			&s.Stats.CostInputTotal, &s.Stats.CostOutputTotal); err != nil {
			return nil, sessionCursor{}, fmt.Errorf("listSessions scan: %w", err)
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, sessionCursor{}, fmt.Errorf("listSessions rows: %w", err)
	}

	var next sessionCursor
	if len(items) > limit {
		last := items[limit-1]
		next = sessionCursor{Sort: sort, Value: def.Extract(last), ID: last.ID}
		items = items[:limit]
	}
	return items, next, nil
}

func sessionTotals(db *sql.DB, d Dialect, f sessionFilters) (SessionTotals, error) {
	// One pass over the filtered sessions JOIN session_events to compute
	// both the session count and the rolled-up token/cost sums. COUNT
	// DISTINCT on s.id stays accurate under the JOIN's row multiplication.
	var q strings.Builder
	q.WriteString(`SELECT
		COUNT(DISTINCT s.id) AS session_count,
		COALESCE(` + countEventTypeExpr(eventTypeLLMResponse) + `, 0) AS llm_call_count,
		COALESCE(` + countEventTypeExpr(eventTypeToolCallResult) + `, 0) AS tool_call_count,
		COALESCE(` + llmAggExpr(d, payloadFieldTokensIn, true) + `, 0) AS tokens_in_total,
		COALESCE(` + llmAggExpr(d, payloadFieldTokensOut, true) + `, 0) AS tokens_out_total,
		COALESCE(` + llmAggExpr(d, payloadFieldCostInput, false) + `, 0) AS cost_input_total,
		COALESCE(` + llmAggExpr(d, payloadFieldCostOutput, false) + `, 0) AS cost_output_total
		FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id WHERE 1=1`)

	var args []any
	applySessionFilters(&q, &args, f)

	var t SessionTotals
	row := db.QueryRow(d.Rebind(q.String()), args...)
	if err := row.Scan(&t.SessionCount, &t.LLMCallCount, &t.ToolCallCount,
		&t.TokensInTotal, &t.TokensOutTotal,
		&t.CostInputTotal, &t.CostOutputTotal); err != nil {
		return SessionTotals{}, fmt.Errorf("sessionTotals: %w", err)
	}
	return t, nil
}

func getSession(db *sql.DB, d Dialect, id string, includeHidden bool) (*SessionDetail, error) {
	var s SessionDetail
	var metadataJSON string
	var q strings.Builder
	q.WriteString(`SELECT s.id, COALESCE(s.entity_id,''), COALESCE(s.group_id,''),
		COALESCE(s.title,''), COALESCE(s.summary,''), COALESCE(s.active_model,''),
		COALESCE(s.metadata,'{}'), s.created_at, s.updated_at, `)
	q.WriteString(sessionStatsSelect(d))
	q.WriteString(` FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id
		WHERE s.id = ?
		GROUP BY s.id, s.entity_id, s.group_id, s.title, s.summary, s.active_model, s.metadata, s.created_at, s.updated_at`)

	row := db.QueryRow(d.Rebind(q.String()), id)
	err := row.Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Title, &s.Summary, &s.ActiveModel,
		&metadataJSON, &s.CreatedAt, &s.UpdatedAt,
		&s.Stats.LLMCallCount, &s.Stats.ToolCallCount,
		&s.Stats.TokensInTotal, &s.Stats.TokensOutTotal,
		&s.Stats.CostInputTotal, &s.Stats.CostOutputTotal)
	if err != nil {
		// Wrap with %w so the handler's errors.Is(sql.ErrNoRows) check
		// for 404 detection still works.
		return nil, fmt.Errorf("getSession scan: %w", err)
	}
	_ = json.Unmarshal([]byte(metadataJSON), &s.Metadata)

	msgs, err := sessionMessages(db, d, id, includeHidden)
	if err != nil {
		return nil, err
	}

	evts, err := sessionEvents(db, d, id)
	if err != nil {
		return nil, err
	}

	annotateAssistantToolCalls(msgs, evts)
	msgs = mergeErrorMessages(msgs, evts)

	s.Messages = msgs
	s.Events = evts

	return &s, nil
}

// annotateAssistantToolCalls copies payload.native_tool_calls_raw from
// each llm_response event onto the corresponding assistant message in
// chronological order. The orchestrator emits exactly one MAIN-LOOP
// llm_response event per assistant message row, in matching order —
// that's the pairing contract, by ordinal rather than by seq (the
// messages table and session_events table maintain independent seq
// sequences).
//
// Side-calls (confirmation classifier, session-title generation, the
// tool-call repair corrector, session summarization) also emit
// llm_response events into the same stream but produce NO assistant
// message row; counting them would shift the pairing for every later
// assistant message in the session. They are recognizable by
// parentage: the orchestrator nests each side-call's
// llm_request/llm_response under a sentinel event — usually named
// `*_invoked` (confirmation_classification_invoked,
// session_title_invoked, tool_call_repair_invoked, …), with one
// naming exception: session summarization parents its side-call under
// `summarization_triggered`. Main-loop responses are never parented
// on a sentinel — so llm_response events whose parent is a sentinel
// are skipped here.
//
// Empty arrays and explicit nulls are omitted: the field exists on
// Message only when the assistant actually invoked tools, so user/tool
// rows and text-only assistant rows stay byte-identical to the
// pre-passthrough contract. If the event payload is malformed or has
// no key, the message simply gets no tool_calls field — the failure
// mode is "missing optional metadata", not "endpoint 500s".
func annotateAssistantToolCalls(msgs []Message, evts []Event) {
	// Ids of side-call sentinel events (`*_invoked`, plus the
	// `summarization_triggered` naming exception); an llm_response
	// parented on one belongs to a side-call, not to an assistant
	// message row.
	sentinels := make(map[string]bool)
	for _, e := range evts {
		if strings.HasSuffix(e.EventType, "_invoked") || e.EventType == "summarization_triggered" {
			sentinels[e.ID] = true
		}
	}
	ei := 0
	for mi := range msgs {
		if msgs[mi].Role != "assistant" {
			continue
		}
		for ei < len(evts) &&
			(evts[ei].EventType != eventTypeLLMResponse ||
				(evts[ei].ParentID != "" && sentinels[evts[ei].ParentID])) {
			ei++
		}
		if ei >= len(evts) {
			return
		}
		var p struct {
			NativeToolCallsRaw json.RawMessage `json:"native_tool_calls_raw"`
		}
		if err := json.Unmarshal(evts[ei].Payload, &p); err == nil && len(p.NativeToolCallsRaw) > 0 {
			tc := bytes.TrimSpace(p.NativeToolCallsRaw)
			if !bytes.Equal(tc, []byte("null")) && !bytes.Equal(tc, []byte("[]")) {
				msgs[mi].ToolCalls = p.NativeToolCallsRaw
			}
		}
		ei++
	}
}

// mergeErrorMessages inserts a synthetic `role:"error"` Message for each
// `llm_error` event that has no matching messages-row. The orchestrator
// writes one messages-row per successful `llm_response` event but writes
// nothing on `llm_error` — so a failed turn shows up as the conversation
// simply ending after the user question with no acknowledgement when the
// diagnostic page renders only `Messages`. The synthetic row makes the
// failure visible without polluting the `messages` table (which the
// orchestrator reads back into the next turn's LLM context).
//
// Pairing is purely timestamp-based: errors are interleaved between real
// messages by chronological comparison of parsed RFC 3339 times. We
// cannot string-compare here: messages.created_at is written by Core's
// `sessions.AddMessage` via `time.Now().Format(time.RFC3339)` (second
// precision) while session_events.ts is microsecond-precision. Within
// the same wall-clock second, a sub-second event ts (`...36.050Z`)
// lex-sorts BEFORE the unfractioned message ts (`...36Z`) because `.`
// (0x2E) < `Z` (0x5A) — which would mis-order the synthetic row.
// time.Parse + time.Before is precision-agnostic. The function
// preserves caller ordering for messages and returns a new slice
// (length grows by the number of unmatched errors); ToolCalls
// annotations done by `annotateAssistantToolCalls` survive intact
// because real messages are copied through by value.
//
// `Seq` on synthetic rows is 0 — a documented sentinel, not a real
// position in the messages table. Consumers must not assume Seq is
// monotonic across the returned slice; ordering is given by slice
// position. `CreatedAt` mirrors the source event's `ts` so the row
// renders at the right point in the chronological view.
func mergeErrorMessages(msgs []Message, evts []Event) []Message {
	type pairedError struct {
		event Event
		t     time.Time
	}
	var errs []pairedError
	for _, e := range evts {
		if e.EventType != eventTypeLLMError {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, e.TS)
		if err != nil {
			// Malformed event ts is upstream-corrupt; skip rather than
			// fall back to lexicographic compare (which would mis-order
			// against second-precision message timestamps).
			continue
		}
		errs = append(errs, pairedError{event: e, t: t})
	}
	if len(errs) == 0 {
		return msgs
	}

	out := make([]Message, 0, len(msgs)+len(errs))
	ei := 0
	for _, m := range msgs {
		mt, err := time.Parse(time.RFC3339Nano, m.CreatedAt)
		if err != nil {
			// Can't place this message chronologically — keep it where
			// it is and don't splice any errors against it.
			out = append(out, m)
			continue
		}
		for ei < len(errs) && errs[ei].t.Before(mt) {
			out = append(out, syntheticErrorMessage(errs[ei].event))
			ei++
		}
		out = append(out, m)
	}
	for ; ei < len(errs); ei++ {
		out = append(out, syntheticErrorMessage(errs[ei].event))
	}
	return out
}

// llmErrorFallback is rendered when the llm_error payload carries no
// usable excerpt — either the payload is missing/malformed or the
// excerpt is empty or exceeds llmErrorExcerptMax. Operators still get a
// clear "this turn failed" signal in the diagnostic page; the technical
// detail is one click away in the Nerd-Mode event payload viewer.
const (
	llmErrorFallback   = "[LLM error] The request could not be completed."
	llmErrorExcerptMax = 200
)

// syntheticErrorMessage builds a virtual messages-row from an llm_error
// event. The fields it sets are: Role (always "error"), Content (derived
// from payload), CreatedAt (mirrors event ts). Seq stays at the zero
// value as a synthetic-row marker — see mergeErrorMessages godoc.
func syntheticErrorMessage(e Event) Message {
	return Message{
		Role:      "error",
		Content:   errorContentFromPayload(e.Payload),
		CreatedAt: e.TS,
	}
}

// errorContentFromPayload extracts a short user-visible string from the
// llm_error payload. Current Core writers stamp `response_body_excerpt`
// (the upstream HTTP body or transport error message); if that's
// present and short enough we inline it so operators can distinguish
// timeouts from rate-limits without opening the event. Long excerpts
// fall back to a static string to keep the bubble UI tidy.
func errorContentFromPayload(payload json.RawMessage) string {
	if len(payload) == 0 {
		return llmErrorFallback
	}
	var p struct {
		ResponseBodyExcerpt string `json:"response_body_excerpt"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return llmErrorFallback
	}
	if p.ResponseBodyExcerpt == "" || len(p.ResponseBodyExcerpt) > llmErrorExcerptMax {
		return llmErrorFallback
	}
	return "[LLM error] " + p.ResponseBodyExcerpt
}

func sessionMessages(db *sql.DB, d Dialect, id string, includeHidden bool) ([]Message, error) {
	// COALESCE(metadata,'') so NULL scans into a Go string (not *string); the
	// metadata column is nullable and absent on every pre-013 row.
	//
	// visibility='hidden' rows are system-injected turns dropped from the
	// user-facing transcript UNLESS the caller opts in: staff analytics pass
	// include_hidden=true to debug them; the customer chat widget does not.
	q := `SELECT seq, role, content, COALESCE(metadata,''), created_at FROM messages WHERE session_id = ?`
	if !includeHidden {
		q += ` AND (visibility IS NULL OR visibility <> 'hidden')`
	}
	q += ` ORDER BY seq`
	rows, err := db.Query(d.Rebind(q), id)
	if err != nil {
		return nil, fmt.Errorf("sessionMessages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	msgs := []Message{}
	for rows.Next() {
		var m Message
		var metadata string
		if err := rows.Scan(&m.Seq, &m.Role, &m.Content, &metadata, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("sessionMessages scan: %w", err)
		}
		// Inline the raw JSON only when present and non-empty — mirror the
		// null/[] guard annotateAssistantToolCalls applies to tool_calls, and
		// keep omitempty rows byte-identical to the pre-013 contract. A
		// malformed value is dropped rather than poisoning the whole envelope.
		if md := strings.TrimSpace(metadata); md != "" && md != "null" && md != "{}" && json.Valid([]byte(md)) {
			m.Metadata = json.RawMessage(md)
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionMessages rows: %w", err)
	}
	return msgs, nil
}

// sessionExists is the 404-probe for handleSessionEvents — a separate
// indexed PK lookup so the polling endpoint can distinguish an
// unknown session id (404) from a caught-up poll (200 with empty
// items). Combining the probe with the events SELECT would force a
// LEFT JOIN or a follow-up "did we get nothing because nothing
// matched or because the session doesn't exist" branch; the dedicated
// probe is one sub-ms PK lookup and stays trivially correct.
func sessionExists(db *sql.DB, d Dialect, id string) (bool, error) {
	var n int
	err := db.QueryRow(d.Rebind(`SELECT 1 FROM sessions WHERE id = ?`), id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sessionExists: %w", err)
	}
	return true, nil
}

// listSessionEventsAsc is the tail-poll read. Single indexed range-scan
// over (session_id, seq) — same index that backs the writer-side
// gap-free seq invariant in opentalon-core. ORDER BY seq is served
// directly from the index (no filesort) and LIMIT clips to one bounded
// page, so the request cost is independent of total session length.
//
// `include_payload=false` drops the payload column entirely from the
// scan rather than nil-ing it post-fetch, so a session with multi-MB
// rows can still be tail-polled cheaply for the structural log.
func listSessionEventsAsc(db *sql.DB, d Dialect, sessionID string, sinceSeq int, includePayload bool, limit int) ([]Event, error) {
	var q strings.Builder
	q.WriteString(`SELECT se.id, se.session_id, se.seq, se.ts, se.event_type,
		COALESCE(se.parent_id,''), COALESCE(se.duration_ms,0)`)
	if includePayload {
		q.WriteString(`, se.payload`)
	}
	q.WriteString(` FROM session_events se
		WHERE se.session_id = ? AND se.seq > ?
		ORDER BY se.seq
		LIMIT ?`)

	rows, err := db.Query(d.Rebind(q.String()), sessionID, sinceSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("listSessionEventsAsc: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]Event, 0, limit)
	for rows.Next() {
		var e Event
		var payload string
		if includePayload {
			if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
				&e.ParentID, &e.DurationMS, &payload); err != nil {
				return nil, fmt.Errorf("listSessionEventsAsc scan (payload): %w", err)
			}
			e.Payload = json.RawMessage(payload)
		} else {
			if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
				&e.ParentID, &e.DurationMS); err != nil {
				return nil, fmt.Errorf("listSessionEventsAsc scan: %w", err)
			}
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listSessionEventsAsc rows: %w", err)
	}
	return items, nil
}

// listSessionDebugEventsAsc reads the raw LLM exchange for one session in
// chronological order. The ORDER BY (ts, id) and WHERE session_id are
// served directly by idx_ai_debug_events_session_ts (the id tiebreaker
// keeps request/response pairs written in the same millisecond stably
// ordered, which the Rails correlation relies on — the request row is the
// 'request' immediately preceding its paired 'response' by ts). COALESCE
// guards the nullable trace_id/status/url; body is NOT NULL so it scans
// straight into a string (error-direction rows hold non-JSON text, hence
// DebugEvent.Body is a string, not json.RawMessage).
func listSessionDebugEventsAsc(db *sql.DB, d Dialect, sessionID string, limit int) ([]DebugEvent, error) {
	rows, err := db.Query(d.Rebind(
		`SELECT id, session_id, COALESCE(trace_id,''), ts, direction,
			COALESCE(status,0), COALESCE(url,''), body
		 FROM ai_debug_events
		 WHERE session_id = ?
		 ORDER BY ts ASC, id ASC
		 LIMIT ?`), sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("listSessionDebugEventsAsc: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]DebugEvent, 0, limit)
	for rows.Next() {
		var e DebugEvent
		var sessionIDOut string
		if err := rows.Scan(&e.ID, &sessionIDOut, &e.TraceID, &e.TS, &e.Direction,
			&e.Status, &e.URL, &e.Body); err != nil {
			return nil, fmt.Errorf("listSessionDebugEventsAsc scan: %w", err)
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listSessionDebugEventsAsc rows: %w", err)
	}
	return items, nil
}

func sessionEvents(db *sql.DB, d Dialect, id string) ([]Event, error) {
	rows, err := db.Query(d.Rebind(
		`SELECT id, session_id, seq, ts, event_type, COALESCE(parent_id,''), COALESCE(duration_ms,0), payload
		 FROM session_events WHERE session_id = ? ORDER BY seq`), id)
	if err != nil {
		return nil, fmt.Errorf("sessionEvents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	evts := []Event{}
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
			&e.ParentID, &e.DurationMS, &payload); err != nil {
			return nil, fmt.Errorf("sessionEvents scan: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		evts = append(evts, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionEvents rows: %w", err)
	}
	return evts, nil
}

// eventListFilters mixes session-shaped filters with event-only filters
// so the /events handler can reuse the same builder set as /sessions
// without polluting sessionFilters with event_type / session_id slots.
type eventListFilters struct {
	Filters        sessionFilters
	SessionID      string
	EventType      string
	IncludePayload bool
}

func listEvents(db *sql.DB, d Dialect, f eventListFilters, cur cursorPair, limit int) ([]Event, cursorPair, error) {
	// Two query shapes — one with JOIN onto sessions (when entity/group
	// filters are set), one direct on session_events (faster, no JOIN
	// row multiplication). The JOIN is conditional because the
	// session_events(event_type, ts) index is the hot path for the
	// "show me all events of type X in window W" case.
	//
	// include_entity_ids and exclude_entity_ids force the JOIN too,
	// since both predicates sit on s.entity_id. A call that *only* sets
	// one of those lists (no entity_id / group_id / event_type) therefore
	// loses the type-index fast path — callers in that shape should pair
	// it with a tight since/until window.
	needJoin := f.Filters.EntityID != "" || f.Filters.GroupID != "" ||
		len(f.Filters.IncludeEntityIDs) > 0 || len(f.Filters.ExcludeEntityIDs) > 0

	var q strings.Builder
	q.WriteString(`SELECT se.id, se.session_id, se.seq, se.ts, se.event_type,
		COALESCE(se.parent_id,''), COALESCE(se.duration_ms,0)`)
	if f.IncludePayload {
		q.WriteString(`, se.payload`)
	}
	q.WriteString(` FROM session_events se`)
	if needJoin {
		q.WriteString(` JOIN sessions s ON s.id = se.session_id`)
	}
	q.WriteString(` WHERE 1=1`)

	var args []any
	if f.SessionID != "" {
		q.WriteString(` AND se.session_id = ?`)
		args = append(args, f.SessionID)
	}
	if f.EventType != "" {
		q.WriteString(` AND se.event_type = ?`)
		args = append(args, f.EventType)
	}
	if needJoin {
		// Full shared filter builder — JOIN is present so entity/group
		// predicates on the `s` alias are valid, and since/until on
		// se.ts is the same axis the no-join branch uses below.
		applySessionFilters(&q, &args, f.Filters)
	} else {
		// No JOIN path: entity/group filters are absent by construction
		// (that's what gates needJoin). Only since/until remain to apply,
		// and they live on se.ts uniformly — same axis applySessionFilters
		// would have used.
		if f.Filters.Since != "" {
			q.WriteString(` AND se.ts >= ?`)
			args = append(args, f.Filters.Since)
		}
		if f.Filters.Until != "" {
			q.WriteString(` AND se.ts < ?`)
			args = append(args, f.Filters.Until)
		}
	}
	if cur.TS != "" {
		q.WriteString(` AND (se.ts < ? OR (se.ts = ? AND se.id < ?))`)
		args = append(args, cur.TS, cur.TS, cur.ID)
	}
	q.WriteString(` ORDER BY se.ts DESC, se.id DESC LIMIT ?`)
	args = append(args, limit+1)

	rows, err := db.Query(d.Rebind(q.String()), args...)
	if err != nil {
		return nil, cursorPair{}, fmt.Errorf("listEvents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]Event, 0, limit)
	for rows.Next() {
		var e Event
		var payload string
		if f.IncludePayload {
			if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
				&e.ParentID, &e.DurationMS, &payload); err != nil {
				return nil, cursorPair{}, fmt.Errorf("listEvents scan (payload): %w", err)
			}
			e.Payload = json.RawMessage(payload)
		} else {
			if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
				&e.ParentID, &e.DurationMS); err != nil {
				return nil, cursorPair{}, fmt.Errorf("listEvents scan: %w", err)
			}
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, cursorPair{}, fmt.Errorf("listEvents rows: %w", err)
	}

	var next cursorPair
	if len(items) > limit {
		last := items[limit-1]
		next = cursorPair{TS: last.TS, ID: last.ID}
		items = items[:limit]
	}
	return items, next, nil
}

// eventsStatsOpts captures the optional shaping params on /events/stats.
// The default zero-value behaves identically to the pre-group_by contract:
// just the top-level aggregates, no buckets.
type eventsStatsOpts struct {
	Filters          sessionFilters
	GroupByEventType bool
	SampleSessions   int    // 0 = omit sample_session_ids; 1..maxSampleSessions otherwise
	BucketBy         string // "" | "day" | "week" | "month" | "year"
	EventType        string // "" = all event types; "llm_response" / "tool_call_result" / ... = restrict
}

// eventsStats is the orchestrator. Three response shapes:
//
//   - Default (no shaping params): top-level aggregates only, filtered
//     on s.created_at — unchanged contract.
//   - group_by=event_type [+ sample_sessions]: top-level + by_event_type[].
//   - bucket_by=day|week|month|year: top-level + time_buckets[], with
//     since/until applied to se.ts on both queries so Σ(buckets) == top.
//
// Each piece is its own SQL — counts and samples could be fused with
// array_agg in Postgres but SQLite's lack of bounded array aggregation
// pushes us to the portable per-query path. Each query is over the same
// indexed JOIN, so the round-trip cost stays modest.
func eventsStats(db *sql.DB, d Dialect, opts eventsStatsOpts) (EventStats, error) {
	stats, err := eventsStatsTotals(db, d, opts.Filters, opts.EventType)
	if err != nil {
		return EventStats{}, err
	}
	if opts.BucketBy != "" {
		// applySessionFilters filters on se.ts uniformly, so totals and
		// buckets see the same row set — Σ(time_buckets[].counter) ==
		// top-level counter for every counter except session_count
		// (where multi-bucket sessions intentionally double-count, see
		// TimeBucket godoc).
		sparse, err := eventsStatsTimeBuckets(db, d, opts.Filters, opts.BucketBy, opts.EventType)
		if err != nil {
			return EventStats{}, err
		}
		stats.TimeBuckets = fillEmptyBuckets(sparse, opts.Filters.Since, opts.Filters.Until, opts.BucketBy)
		return stats, nil
	}
	if !opts.GroupByEventType {
		return stats, nil
	}
	buckets, err := eventsStatsByEventType(db, d, opts.Filters)
	if err != nil {
		return EventStats{}, err
	}
	if opts.SampleSessions > 0 {
		if err := fillSampleSessionIDs(db, d, opts.Filters, buckets, opts.SampleSessions); err != nil {
			return EventStats{}, err
		}
	}
	// Note: when group_by is set but the filtered window has no events,
	// buckets is nil and `omitempty` drops the field — the response then
	// looks identical to an un-grouped call. That's fine: the consumer
	// authored the request and already knows they asked for grouping; a
	// "present-but-empty" contract would require pointer-to-slice for one
	// edge case and isn't worth the API ergonomics cost.
	stats.ByEventType = buckets
	return stats, nil
}

// eventsStatsTotals is the cross-session aggregate. Same JIT pattern as
// sessionTotals but without the per-session GROUP BY. Always JOINs
// session_events so applySessionFilters' se.ts predicate works — when
// no time filter is set the LEFT JOIN keeps sessions-without-events in
// the count too.
//
// eventType (optional) restricts to a single event_type — narrows
// event_count to that type, and zeroes the llm/tool sub-counters if
// they don't match (e.g. event_type=tool_call_result → llm_call_count
// = 0, tokens_* = 0). session_count counts distinct sessions that have
// at least one event of the requested type in the window.
func eventsStatsTotals(db *sql.DB, d Dialect, f sessionFilters, eventType string) (EventStats, error) {
	var q strings.Builder
	q.WriteString(`SELECT
		COUNT(DISTINCT s.id) AS session_count,
		COUNT(se.id) AS event_count,
		COALESCE(` + countEventTypeExpr(eventTypeLLMResponse) + `, 0),
		COALESCE(` + countEventTypeExpr(eventTypeToolCallResult) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldTokensIn, true) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldTokensOut, true) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldCostInput, false) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldCostOutput, false) + `, 0)
		FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id WHERE 1=1`)

	var args []any
	applySessionFilters(&q, &args, f)
	applyEventTypeFilter(&q, &args, eventType)

	var stats EventStats
	row := db.QueryRow(d.Rebind(q.String()), args...)
	if err := row.Scan(&stats.SessionCount, &stats.EventCount,
		&stats.LLMCallCount, &stats.ToolCallCount,
		&stats.TokensInTotal, &stats.TokensOutTotal,
		&stats.CostInputTotal, &stats.CostOutputTotal); err != nil {
		return EventStats{}, fmt.Errorf("eventsStatsTotals: %w", err)
	}
	return stats, nil
}

// eventsStatsTimeBuckets returns one row per bucket-start key in the
// filtered window that has at least one event. since/until apply via
// applySessionFilters (uniform on se.ts across the plugin), so totals
// and buckets see the same row set — Σ(time_buckets[].counter) ==
// top-level counter for every counter except session_count.
//
// The slice is sparse (missing buckets aren't returned); the caller
// runs it through fillEmptyBuckets to produce a continuous x-axis.
// Ordering is ASC by bucket so the fill loop can do a single sorted
// merge instead of building a full lookup map.
func eventsStatsTimeBuckets(db *sql.DB, d Dialect, f sessionFilters, granularity, eventType string) ([]TimeBucket, error) {
	bucketExpr := d.DateBucket("se.ts", granularity)

	var q strings.Builder
	fmt.Fprintf(&q, `SELECT %s AS bucket,
		COUNT(DISTINCT s.id),
		COUNT(se.id),
		COALESCE(%s, 0),
		COALESCE(%s, 0),
		COALESCE(%s, 0),
		COALESCE(%s, 0),
		COALESCE(%s, 0),
		COALESCE(%s, 0)
		FROM sessions s JOIN session_events se ON se.session_id = s.id WHERE 1=1`,
		bucketExpr,
		countEventTypeExpr(eventTypeLLMResponse),
		countEventTypeExpr(eventTypeToolCallResult),
		llmAggExpr(d, payloadFieldTokensIn, true),
		llmAggExpr(d, payloadFieldTokensOut, true),
		llmAggExpr(d, payloadFieldCostInput, false),
		llmAggExpr(d, payloadFieldCostOutput, false),
	)

	var args []any
	applySessionFilters(&q, &args, f)
	applyEventTypeFilter(&q, &args, eventType)

	// GROUP BY the same expression we project, ORDER BY the bucket
	// alias. SQLite accepts both forms; Postgres requires the alias or
	// the full expression — alias is more readable.
	fmt.Fprintf(&q, ` GROUP BY %s ORDER BY bucket ASC`, bucketExpr)

	rows, err := db.Query(d.Rebind(q.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("eventsStatsTimeBuckets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []TimeBucket
	for rows.Next() {
		var b TimeBucket
		if err := rows.Scan(&b.Bucket, &b.SessionCount, &b.EventCount,
			&b.LLMCallCount, &b.ToolCallCount,
			&b.TokensInTotal, &b.TokensOutTotal,
			&b.CostInputTotal, &b.CostOutputTotal); err != nil {
			return nil, fmt.Errorf("eventsStatsTimeBuckets scan: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventsStatsTimeBuckets rows: %w", err)
	}
	return out, nil
}

// fillEmptyBuckets expands a SQL-returned sparse bucket slice into a
// continuous one covering every bucket-start key in [since, until).
// Empty buckets get zero counters so the consumer's chart x-axis is
// continuous without a client-side fill loop.
//
// Bucket-key iteration mirrors the SQL DateBucket truncation, so a
// bucket present in the DB always lines up with one the iterator
// produces. Both since and until are pre-validated RFC3339; an
// unparseable value (shouldn't happen post-validation) yields an empty
// result — the route handler will return whatever the SQL returned
// rather than crash on a malformed timestamp.
func fillEmptyBuckets(sparse []TimeBucket, since, until, granularity string) []TimeBucket {
	sinceT, err1 := time.Parse(time.RFC3339, since)
	untilT, err2 := time.Parse(time.RFC3339, until)
	if err1 != nil || err2 != nil {
		return sparse
	}

	have := make(map[string]TimeBucket, len(sparse))
	for _, b := range sparse {
		have[b.Bucket] = b
	}

	out := make([]TimeBucket, 0, len(sparse))
	for cur := truncateToBucket(sinceT, granularity); cur.Before(untilT); cur = advanceBucket(cur, granularity) {
		key := cur.Format("2006-01-02")
		if b, ok := have[key]; ok {
			out = append(out, b)
		} else {
			out = append(out, TimeBucket{Bucket: key})
		}
	}
	return out
}

// eventsStatsByEventType returns one row per distinct event_type in the
// filtered window. INNER JOIN (not LEFT JOIN like the totals query) —
// sessions with zero events don't contribute to any bucket by design.
// Ordering is deterministic (count DESC, event_type ASC) so consumers
// can diff/concatenate across windows.
func eventsStatsByEventType(db *sql.DB, d Dialect, f sessionFilters) ([]EventTypeBucket, error) {
	var q strings.Builder
	q.WriteString(`SELECT se.event_type, COUNT(*) AS c
		FROM sessions s JOIN session_events se ON se.session_id = s.id WHERE 1=1`)
	var args []any
	applySessionFilters(&q, &args, f)
	q.WriteString(` GROUP BY se.event_type ORDER BY c DESC, se.event_type ASC`)

	rows, err := db.Query(d.Rebind(q.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("eventsStatsByEventType: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var buckets []EventTypeBucket
	for rows.Next() {
		var b EventTypeBucket
		if err := rows.Scan(&b.EventType, &b.Count); err != nil {
			return nil, fmt.Errorf("eventsStatsByEventType scan: %w", err)
		}
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventsStatsByEventType rows: %w", err)
	}
	return buckets, nil
}

// fillSampleSessionIDs populates SampleSessionIDs on each bucket with the
// most-recent N distinct session IDs that contain at least one event of
// that type in the filtered window.
//
// One query per bucket (rather than a single window-function query). The
// bucket count is bounded by the number of distinct event types in the
// filtered window (~24 at the documented top end) and each query is a
// small index scan on (event_type, ts), so the worst case is ~24 cheap
// lookups in exchange for a portable cross-dialect implementation that
// avoids array_agg/string_agg dialect drift.
//
// The per-bucket query is delegated to sampleSessionIDsForBucket so each
// iteration owns a defer-scoped rows.Close — putting the defer inside
// this loop directly would stack closes across all ~24 iterations and
// hold connection-pool slots until the outer function returns.
func fillSampleSessionIDs(db *sql.DB, d Dialect, f sessionFilters, buckets []EventTypeBucket, n int) error {
	for i := range buckets {
		ids, err := sampleSessionIDsForBucket(db, d, f, buckets[i].EventType, n)
		if err != nil {
			return err
		}
		buckets[i].SampleSessionIDs = ids
	}
	return nil
}

// sampleSessionIDsForBucket returns up to n distinct session IDs that
// contain at least one event of the given type in the filtered window,
// ordered by most-recent-event-first. Same row-shape contract as the
// inline pre-refactor query — extracted purely so the defer-pattern
// matches the rest of the file (no defer-in-loop).
func sampleSessionIDsForBucket(db *sql.DB, d Dialect, f sessionFilters, eventType string, n int) ([]string, error) {
	var q strings.Builder
	// Inner GROUP BY collapses to one row per (event_type, session_id)
	// pair with the most-recent ts in that pair; the outer ORDER BY
	// then ranks sessions by recency within the bucket.
	q.WriteString(`SELECT session_id FROM (
		SELECT se.session_id, MAX(se.ts) AS last_ts
		FROM sessions s JOIN session_events se ON se.session_id = s.id
		WHERE se.event_type = ?`)
	args := []any{eventType}
	applySessionFilters(&q, &args, f)
	q.WriteString(` GROUP BY se.session_id) sub ORDER BY last_ts DESC, session_id LIMIT ?`)
	args = append(args, n)

	rows, err := db.Query(d.Rebind(q.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("sampleSessionIDsForBucket[%s]: %w", eventType, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, fmt.Errorf("sampleSessionIDsForBucket scan: %w", err)
		}
		ids = append(ids, sid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sampleSessionIDsForBucket rows: %w", err)
	}
	return ids, nil
}

func getPromptSnapshot(db *sql.DB, d Dialect, sha string) (*PromptSnapshot, error) {
	var p PromptSnapshot
	err := db.QueryRow(
		d.Rebind(`SELECT sha256, kind, content, created_at FROM prompt_snapshots WHERE sha256 = ?`), sha,
	).Scan(&p.SHA256, &p.Kind, &p.Content, &p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}
