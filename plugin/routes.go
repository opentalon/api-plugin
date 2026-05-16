// Package plugin implements the api-plugin HTTP surface — a read-only
// REST API over OpenTalon's sessions, session_events, and prompt_snapshots
// tables. Routes are intentionally narrow: the Rails review UI is the
// primary consumer, and the upstream contract is "five endpoints, JIT
// SQL aggregation, no denormalised counters". See README.md for the
// per-endpoint shape and the design memo for the rationale.
package plugin

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

// SessionListItem is one row of GET /sessions.
type SessionListItem struct {
	ID          string       `json:"id"`
	EntityID    string       `json:"entity_id,omitempty"`
	GroupID     string       `json:"group_id,omitempty"`
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
type Message struct {
	Seq       int    `json:"seq"`
	Role      string `json:"role"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
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

// maxSampleSessions caps ?sample_sessions=N on /events/stats. 5 covers
// the "give me a couple of examples to seed a drill-down" use case while
// bounding the per-bucket secondary query and the response size — at
// the documented ~24-event-type ceiling and 5 IDs per bucket the upper
// bound is 120 small ID strings.
const maxSampleSessions = 5

// Event-type constants mirror events.Type* in opentalon. Keeping the
// strings local (rather than importing the opentalon Go package just to
// reference them) avoids coupling api-plugin's release cadence to
// opentalon's — if a new event type is added upstream we read it
// through anyway, we just don't pre-classify it here.
const (
	eventTypeLLMResponse    = "llm_response"
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
func limitFromQuery(r *http.Request) int {
	v, err := strconv.Atoi(r.URL.Query().Get("limit"))
	switch {
	case err != nil || v <= 0:
		return defaultLimit
	case v > maxLimit:
		return maxLimit
	default:
		return v
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
	ExcludeEntityIDs []string // entity_ids to exclude from rows AND aggregation
	Since            string   // RFC3339, empty = no lower bound
	Until            string   // RFC3339, empty = no upper bound
}

func filtersFromQuery(r *http.Request) (sessionFilters, error) {
	since, until, err := timeRangeFromQuery(r)
	if err != nil {
		return sessionFilters{}, err
	}
	excludeIDs, err := parseCommaList(r.URL.Query().Get("exclude_entity_ids"), maxExcludeEntityIDs)
	if err != nil {
		return sessionFilters{}, fmt.Errorf("exclude_entity_ids: %w", err)
	}
	return sessionFilters{
		EntityID:         r.URL.Query().Get("entity_id"),
		GroupID:          r.URL.Query().Get("group_id"),
		ExcludeEntityIDs: excludeIDs,
		Since:            since,
		Until:            until,
	}, nil
}

// maxExcludeEntityIDs caps the exclude_entity_ids list. Realistic callers
// hide a handful of internal/staff actors; the cap protects against a
// pathological URL that would (a) hit SQLite's ~999 bind-param ceiling
// and (b) produce a NOT IN with thousands of placeholders.
const maxExcludeEntityIDs = 200

// --- /sessions sort ---

// Sort-key constants are duplicated as the public query-string vocabulary,
// so the keys also appear in the README. Add a new key in three places:
// the constant block, sessionSortFromQuery's allow-list, and
// resolveSessionSort's switch.
const (
	sortKeyCreatedAt     = "created_at"
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
	case sortKeyCreatedAt, sortKeyCostTotal, sortKeyLLMCallCount,
		sortKeyToolCallCount, sortKeyTokensIn, sortKeyTokensOut:
	default:
		return sessionSort{}, fmt.Errorf(
			"sort: unsupported key %q (allowed: %s, %s, %s, %s, %s, %s)",
			s, sortKeyCreatedAt, sortKeyCostTotal, sortKeyLLMCallCount,
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
	case sortKeyCostTotal:
		def.Expr = llmAggExpr(d, payloadFieldCostInput, false) + " + " + llmAggExpr(d, payloadFieldCostOutput, false)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string {
			return strconv.FormatFloat(r.Stats.CostInputTotal+r.Stats.CostOutputTotal, 'f', -1, 64)
		}
		def.BindValue = func(raw string) (any, error) { return strconv.ParseFloat(raw, 64) }
	case sortKeyLLMCallCount:
		def.Expr = fmt.Sprintf("SUM(CASE WHEN se.event_type = '%s' THEN 1 ELSE 0 END)", eventTypeLLMResponse)
		def.IsAggregate = true
		def.Extract = func(r SessionListItem) string { return strconv.Itoa(r.Stats.LLMCallCount) }
		def.BindValue = func(raw string) (any, error) { return strconv.ParseInt(raw, 10, 64) }
	case sortKeyToolCallCount:
		def.Expr = fmt.Sprintf("SUM(CASE WHEN se.event_type = '%s' THEN 1 ELSE 0 END)", eventTypeToolCallResult)
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
	limit := limitFromQuery(r)

	items, next, err := listSessions(h.db, h.dialect, f, sort, cur, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	totals, err := sessionTotals(h.db, h.dialect, f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
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
	sess, err := getSession(h.db, h.dialect, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "session not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, sess)
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
	limit := limitFromQuery(r)
	includePayload := r.URL.Query().Get("include_payload") != "false"

	q := r.URL.Query()
	items, next, err := listEvents(h.db, h.dialect, eventListFilters{
		Filters:        f,
		SessionID:      q.Get("session_id"),
		EventType:      q.Get("event_type"),
		IncludePayload: includePayload,
	}, cur, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
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
		writeErr(w, http.StatusInternalServerError, err.Error())
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
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, snap)
}

// --- Query functions ---

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
		fmt.Sprintf("COALESCE(SUM(CASE WHEN se.event_type = '%s' THEN 1 ELSE 0 END), 0) AS llm_call_count", eventTypeLLMResponse),
		fmt.Sprintf("COALESCE(SUM(CASE WHEN se.event_type = '%s' THEN 1 ELSE 0 END), 0) AS tool_call_count", eventTypeToolCallResult),
		"COALESCE(" + llmAggExpr(d, payloadFieldTokensIn, true) + ", 0) AS tokens_in_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldTokensOut, true) + ", 0) AS tokens_out_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldCostInput, false) + ", 0) AS cost_input_total",
		"COALESCE(" + llmAggExpr(d, payloadFieldCostOutput, false) + ", 0) AS cost_output_total",
	}, ", ")
}

// applySessionFilters appends shared filter predicates and args. The
// sessions table is always aliased as "s" so the JOIN-onto-events queries
// and the totals query (no JOIN) share this builder without conflict.
func applySessionFilters(q *strings.Builder, args *[]any, f sessionFilters) {
	if f.EntityID != "" {
		q.WriteString(" AND s.entity_id = ?")
		*args = append(*args, f.EntityID)
	}
	if f.GroupID != "" {
		q.WriteString(" AND s.group_id = ?")
		*args = append(*args, f.GroupID)
	}
	// Hits both /sessions (rows + totals) and /events/stats so a caller
	// filtering "internal users" sees a coherent count and aggregate —
	// visible rows always sum to the displayed totals.
	writeInClause(q, args, "s.entity_id", f.ExcludeEntityIDs, true)
	if f.Since != "" {
		q.WriteString(" AND s.created_at >= ?")
		*args = append(*args, f.Since)
	}
	if f.Until != "" {
		q.WriteString(" AND s.created_at < ?")
		*args = append(*args, f.Until)
	}
}

func listSessions(db *sql.DB, d Dialect, f sessionFilters, sort sessionSort, cur sessionCursor, limit int) ([]SessionListItem, sessionCursor, error) {
	def := resolveSessionSort(sort, d)

	var q strings.Builder
	q.WriteString(`SELECT s.id, COALESCE(s.entity_id,''), COALESCE(s.group_id,''),
		COALESCE(s.summary,''), COALESCE(s.active_model,''),
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

	q.WriteString(` GROUP BY s.id, s.entity_id, s.group_id, s.summary, s.active_model, s.created_at, s.updated_at`)
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
		if err := rows.Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Summary, &s.ActiveModel,
			&s.CreatedAt, &s.UpdatedAt,
			&s.Stats.LLMCallCount, &s.Stats.ToolCallCount,
			&s.Stats.TokensInTotal, &s.Stats.TokensOutTotal,
			&s.Stats.CostInputTotal, &s.Stats.CostOutputTotal); err != nil {
			return nil, sessionCursor{}, fmt.Errorf("listSessions scan: %w", err)
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, sessionCursor{}, err
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
		COALESCE(SUM(CASE WHEN se.event_type = '`)
	q.WriteString(eventTypeLLMResponse)
	q.WriteString(`' THEN 1 ELSE 0 END), 0) AS llm_call_count,
		COALESCE(SUM(CASE WHEN se.event_type = '`)
	q.WriteString(eventTypeToolCallResult)
	q.WriteString(`' THEN 1 ELSE 0 END), 0) AS tool_call_count,
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

func getSession(db *sql.DB, d Dialect, id string) (*SessionDetail, error) {
	var s SessionDetail
	var metadataJSON string
	var q strings.Builder
	q.WriteString(`SELECT s.id, COALESCE(s.entity_id,''), COALESCE(s.group_id,''),
		COALESCE(s.summary,''), COALESCE(s.active_model,''),
		COALESCE(s.metadata,'{}'), s.created_at, s.updated_at, `)
	q.WriteString(sessionStatsSelect(d))
	q.WriteString(` FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id
		WHERE s.id = ?
		GROUP BY s.id, s.entity_id, s.group_id, s.summary, s.active_model, s.metadata, s.created_at, s.updated_at`)

	row := db.QueryRow(d.Rebind(q.String()), id)
	err := row.Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Summary, &s.ActiveModel,
		&metadataJSON, &s.CreatedAt, &s.UpdatedAt,
		&s.Stats.LLMCallCount, &s.Stats.ToolCallCount,
		&s.Stats.TokensInTotal, &s.Stats.TokensOutTotal,
		&s.Stats.CostInputTotal, &s.Stats.CostOutputTotal)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(metadataJSON), &s.Metadata)

	msgs, err := sessionMessages(db, d, id)
	if err != nil {
		return nil, err
	}
	s.Messages = msgs

	evts, err := sessionEvents(db, d, id)
	if err != nil {
		return nil, err
	}
	s.Events = evts

	return &s, nil
}

func sessionMessages(db *sql.DB, d Dialect, id string) ([]Message, error) {
	rows, err := db.Query(d.Rebind(
		`SELECT seq, role, content, created_at FROM messages WHERE session_id = ? ORDER BY seq`), id)
	if err != nil {
		return nil, fmt.Errorf("sessionMessages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	msgs := []Message{}
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.Seq, &m.Role, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
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
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		evts = append(evts, e)
	}
	return evts, rows.Err()
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
	// exclude_entity_ids forces the JOIN too, since the predicate sits
	// on s.entity_id. A call that *only* sets exclude_entity_ids
	// (no entity_id / group_id / event_type) therefore loses the
	// type-index fast path — callers in that shape should pair it with
	// a tight since/until window.
	needJoin := f.Filters.EntityID != "" || f.Filters.GroupID != "" || len(f.Filters.ExcludeEntityIDs) > 0

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
	if f.Filters.Since != "" {
		q.WriteString(` AND se.ts >= ?`)
		args = append(args, f.Filters.Since)
	}
	if f.Filters.Until != "" {
		q.WriteString(` AND se.ts < ?`)
		args = append(args, f.Filters.Until)
	}
	if needJoin {
		if f.Filters.EntityID != "" {
			q.WriteString(` AND s.entity_id = ?`)
			args = append(args, f.Filters.EntityID)
		}
		if f.Filters.GroupID != "" {
			q.WriteString(` AND s.group_id = ?`)
			args = append(args, f.Filters.GroupID)
		}
		writeInClause(&q, &args, "s.entity_id", f.Filters.ExcludeEntityIDs, true)
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
				return nil, cursorPair{}, err
			}
			e.Payload = json.RawMessage(payload)
		} else {
			if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &e.TS, &e.EventType,
				&e.ParentID, &e.DurationMS); err != nil {
				return nil, cursorPair{}, err
			}
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, cursorPair{}, err
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
	SampleSessions   int // 0 = omit sample_session_ids; 1..maxSampleSessions otherwise
}

// eventsStats is the orchestrator: top-level totals always, plus an
// optional per-event_type breakdown, plus optional per-bucket session
// samples. Each piece is its own SQL — counts and samples could be
// fused with array_agg in Postgres but SQLite's lack of bounded array
// aggregation pushes us to the portable two-query path. Each query is
// over the same indexed JOIN, so the round-trip cost stays modest.
func eventsStats(db *sql.DB, d Dialect, opts eventsStatsOpts) (EventStats, error) {
	stats, err := eventsStatsTotals(db, d, opts.Filters)
	if err != nil {
		return EventStats{}, err
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

func eventsStatsTotals(db *sql.DB, d Dialect, f sessionFilters) (EventStats, error) {
	// Cross-session aggregate; same JIT pattern as sessionTotals but
	// without the per-session GROUP BY. Always JOINs sessions so
	// entity/group filters work — when neither is set the JOIN is still
	// cheap (PK side).
	var q strings.Builder
	q.WriteString(`SELECT
		COUNT(DISTINCT s.id) AS session_count,
		COUNT(se.id) AS event_count,
		COALESCE(SUM(CASE WHEN se.event_type = '` + eventTypeLLMResponse + `' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN se.event_type = '` + eventTypeToolCallResult + `' THEN 1 ELSE 0 END), 0),
		COALESCE(` + llmAggExpr(d, payloadFieldTokensIn, true) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldTokensOut, true) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldCostInput, false) + `, 0),
		COALESCE(` + llmAggExpr(d, payloadFieldCostOutput, false) + `, 0)
		FROM sessions s LEFT JOIN session_events se ON se.session_id = s.id WHERE 1=1`)

	var args []any
	applySessionFilters(&q, &args, f)

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
func fillSampleSessionIDs(db *sql.DB, d Dialect, f sessionFilters, buckets []EventTypeBucket, n int) error {
	for i := range buckets {
		var q strings.Builder
		// Inner GROUP BY collapses to one row per (event_type, session_id)
		// pair with the most-recent ts in that pair; the outer ORDER BY
		// then ranks sessions by recency within the bucket.
		q.WriteString(`SELECT session_id FROM (
			SELECT se.session_id, MAX(se.ts) AS last_ts
			FROM sessions s JOIN session_events se ON se.session_id = s.id
			WHERE se.event_type = ?`)
		args := []any{buckets[i].EventType}
		applySessionFilters(&q, &args, f)
		q.WriteString(` GROUP BY se.session_id) sub ORDER BY last_ts DESC, session_id LIMIT ?`)
		args = append(args, n)

		rows, err := db.Query(d.Rebind(q.String()), args...)
		if err != nil {
			return fmt.Errorf("fillSampleSessionIDs[%s]: %w", buckets[i].EventType, err)
		}
		for rows.Next() {
			var sid string
			if err := rows.Scan(&sid); err != nil {
				_ = rows.Close()
				return fmt.Errorf("fillSampleSessionIDs scan: %w", err)
			}
			buckets[i].SampleSessionIDs = append(buckets[i].SampleSessionIDs, sid)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("fillSampleSessionIDs rows: %w", err)
		}
	}
	return nil
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
