package plugin

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// setupTestDB builds an in-memory SQLite mirroring the opentalon schema
// surface that api-plugin reads from: sessions, messages, session_events,
// prompt_snapshots. DDL is intentionally minimal — only the columns and
// indexes the api-plugin queries.
func setupTestDB(t *testing.T) (*sql.DB, Dialect) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}

	for _, ddl := range []string{
		`CREATE TABLE sessions (
			id TEXT PRIMARY KEY,
			summary TEXT,
			title TEXT,
			active_model TEXT,
			metadata TEXT,
			entity_id TEXT DEFAULT '',
			group_id TEXT DEFAULT '',
			created_at TEXT,
			updated_at TEXT
		)`,
		`CREATE TABLE messages (session_id TEXT, seq INTEGER, role TEXT, content TEXT, metadata TEXT, visibility TEXT, created_at TEXT)`,
		`CREATE UNIQUE INDEX idx_messages_session_seq ON messages(session_id, seq)`,
		`CREATE TABLE session_events (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			ts TEXT NOT NULL,
			event_type TEXT NOT NULL,
			parent_id TEXT,
			duration_ms INTEGER,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		// Mirrors opentalon migration 009_session_events.sql:43 — the
		// composite index that backs both the writer-side gap-free seq
		// invariant AND the index-served ORDER BY seq on
		// /sessions/{id}/events. Without it, the tail-poll perf claim
		// ("one indexed range-scan, no filesort") is untested.
		`CREATE UNIQUE INDEX idx_session_events_session_seq ON session_events(session_id, seq)`,
		`CREATE TABLE prompt_snapshots (
			sha256 TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE ai_debug_events (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			trace_id TEXT,
			ts TEXT NOT NULL,
			direction TEXT NOT NULL,
			status INTEGER,
			url TEXT,
			body TEXT NOT NULL
		)`,
		// Mirrors the opentalon migration index that backs the
		// session_id WHERE + (ts, id) ORDER BY on /sessions/{id}/debug-events.
		`CREATE INDEX idx_ai_debug_events_session_ts ON ai_debug_events(session_id, ts)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("DDL: %v", err)
		}
	}

	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Two sessions, two groups. Session A (older) has 2 llm_response events
	// with priced payloads; Session B (newer) has 1 llm_response + 1
	// tool_call_result. Time stamps are chosen so created_at DESC gives B
	// first and the cursor test below can walk forward into A.
	exec(`INSERT INTO sessions VALUES ('sess_a','first session','First chat title','gpt-4o','{}','user_1','group_x','2024-01-01T10:00:00Z','2024-01-01T10:30:00Z')`)
	exec(`INSERT INTO sessions VALUES ('sess_b','second session',NULL,'gpt-4o','{"locale":"de"}','user_2','group_y','2024-02-01T10:00:00Z','2024-02-01T10:30:00Z')`)

	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_a',1,'user','hi','2024-01-01T10:00:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_a',2,'assistant','hello','2024-01-01T10:00:01Z')`)

	// llm_response payloads — tokens + cost matching what the opentalon
	// provider wrapper stamps via the cost-tracking PR. Two events on
	// sess_a (different costs) to exercise SUM correctness.
	llmPayload := func(tokIn, tokOut int, costIn, costOut float64) string {
		return fmt.Sprintf(`{"v":1,"raw_content_excerpt":"x","tokens_in":%d,"tokens_out":%d,"cost_input":%g,"cost_output":%g,"latency_ms":100}`,
			tokIn, tokOut, costIn, costOut)
	}
	exec(`INSERT INTO session_events VALUES ('evt_a1','sess_a',1,'2024-01-01T10:00:00Z','llm_response',NULL,100,?,'2024-01-01T10:00:00Z')`,
		llmPayload(1000, 500, 0.0025, 0.005))
	exec(`INSERT INTO session_events VALUES ('evt_a2','sess_a',2,'2024-01-01T10:10:00Z','llm_response',NULL,150,?,'2024-01-01T10:10:00Z')`,
		llmPayload(2000, 1000, 0.005, 0.010))
	exec(`INSERT INTO session_events VALUES ('evt_b1','sess_b',1,'2024-02-01T10:00:00Z','llm_response',NULL,200,?,'2024-02-01T10:00:00Z')`,
		llmPayload(500, 250, 0.00125, 0.0025))
	exec(`INSERT INTO session_events VALUES ('evt_b2','sess_b',2,'2024-02-01T10:00:10Z','tool_call_result','evt_b1',50,'{"v":1,"result":"ok"}','2024-02-01T10:00:10Z')`)

	exec(`INSERT INTO prompt_snapshots VALUES ('sha_sys_1','system_prompt','You are a helpful assistant.','2024-01-01T00:00:00Z')`)

	// Raw debug rows on sess_a: a request (single JSON), a STREAMED response
	// stored verbatim as the SSE wire stream, and an error row whose body is a
	// non-JSON "Class: message" diagnostic. Distinct ts so order is testable.
	// sess_b has none (exercises the "session exists, no debug rows" path).
	exec(`INSERT INTO ai_debug_events VALUES ('dbg_a1','sess_a','tr_1','2024-01-01T10:00:00Z','request',NULL,'https://provider/v1/chat/completions','{"model":"gpt-oss-120b"}')`)
	exec(`INSERT INTO ai_debug_events VALUES ('dbg_a2','sess_a','tr_1','2024-01-01T10:00:01Z','response',200,'https://provider/v1/chat/completions',?)`,
		"data: {\"id\":\"chatcmpl-x\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\ndata: [DONE]\n")
	exec(`INSERT INTO ai_debug_events VALUES ('dbg_a3','sess_a','tr_2','2024-01-01T10:00:02Z','error',NULL,NULL,'Net::ReadTimeout: execution expired')`)

	t.Cleanup(func() { _ = db.Close() })
	return db, sqliteDialect
}

func mustUnmarshal(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, string(data))
	}
}

func newTestHandler(t *testing.T) *Handler {
	db, dialect := setupTestDB(t)
	// The in-memory test DB is writable; point both pools at it so the single
	// mutating endpoint (PATCH /sessions/{id}) works under test. In production
	// these are two pools (read-only + read-write) — see Handler.Configure.
	return &Handler{db: db, writeDB: db, dialect: dialect}
}

func do(t *testing.T, h *Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest("GET", target, nil))
	return w
}

// doReq issues an arbitrary method/body request (e.g. PATCH with a JSON body).
func doReq(t *testing.T, h *Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest(method, target, strings.NewReader(body)))
	return w
}

// mustStatus fails the test if the response status doesn't match. Used
// in new tests to cut the repeated "if w.Code != X { t.Fatalf(...) }"
// boilerplate; existing tests left on the verbose form for now (mass
// conversion is a separate cleanup).
func mustStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, want, w.Body.String())
	}
}

func TestHealth(t *testing.T) {
	w := do(t, newTestHandler(t), "/health")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestListSessions_StatsRollupPerRow(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 {
		t.Fatalf("got %d sessions, want 2", len(resp.Items))
	}
	// Newest first: sess_b, then sess_a.
	if resp.Items[0].ID != "sess_b" {
		t.Errorf("order: items[0] = %q, want sess_b (newest first)", resp.Items[0].ID)
	}
	// sess_a aggregates: 2 llm_response events, sum(tokens_in) = 3000, sum(tokens_out) = 1500,
	// sum(cost_input) = 0.0075, sum(cost_output) = 0.015.
	a := resp.Items[1]
	if a.Stats.LLMCallCount != 2 {
		t.Errorf("sess_a llm_call_count = %d, want 2", a.Stats.LLMCallCount)
	}
	if a.Stats.TokensInTotal != 3000 {
		t.Errorf("sess_a tokens_in_total = %d, want 3000", a.Stats.TokensInTotal)
	}
	if a.Stats.TokensOutTotal != 1500 {
		t.Errorf("sess_a tokens_out_total = %d, want 1500", a.Stats.TokensOutTotal)
	}
	if a.Stats.CostInputTotal != 0.0075 {
		t.Errorf("sess_a cost_input_total = %v, want 0.0075", a.Stats.CostInputTotal)
	}
	if a.Stats.CostOutputTotal != 0.015 {
		t.Errorf("sess_a cost_output_total = %v, want 0.015", a.Stats.CostOutputTotal)
	}
	// sess_b: 1 llm_response (tool_call_result is counted separately), 1 tool_call_result.
	b := resp.Items[0]
	if b.Stats.LLMCallCount != 1 {
		t.Errorf("sess_b llm_call_count = %d, want 1", b.Stats.LLMCallCount)
	}
	if b.Stats.ToolCallCount != 1 {
		t.Errorf("sess_b tool_call_count = %d, want 1", b.Stats.ToolCallCount)
	}
}

func TestListSessions_TotalsCoverFullFilteredSet(t *testing.T) {
	// Totals must reflect every matching session, not just the current
	// page — the review UI shows "monthly cost across N sessions" above
	// a 25-row table.
	h := newTestHandler(t)
	w := do(t, h, "/sessions?limit=1")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 {
		t.Fatalf("got %d items, want 1 (paginated)", len(resp.Items))
	}
	if resp.Totals.SessionCount != 2 {
		t.Errorf("Totals.SessionCount = %d, want 2 (across full filtered set)", resp.Totals.SessionCount)
	}
	// Totals: 3 llm_response events across both sessions.
	if resp.Totals.LLMCallCount != 3 {
		t.Errorf("Totals.LLMCallCount = %d, want 3", resp.Totals.LLMCallCount)
	}
	wantTokensIn := int64(3000 + 500)
	if resp.Totals.TokensInTotal != wantTokensIn {
		t.Errorf("Totals.TokensInTotal = %d, want %d", resp.Totals.TokensInTotal, wantTokensIn)
	}
}

func TestListSessions_FilterByEntityAndGroup(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?entity_id=user_1&group_id=group_x")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_a" {
		t.Fatalf("got %+v, want exactly sess_a", resp.Items)
	}
}

func TestListSessions_CursorPagination(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/sessions?limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor must be set when more rows remain")
	}
	if page1.Items[0].ID != "sess_b" {
		t.Errorf("page 1: items[0] = %q, want sess_b", page1.Items[0].ID)
	}

	w2 := do(t, h, "/sessions?limit=1&cursor="+page1.NextCursor)
	var page2 SessionListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if page2.Items[0].ID != "sess_a" {
		t.Errorf("page 2: items[0] = %q, want sess_a", page2.Items[0].ID)
	}
	if page2.NextCursor != "" {
		t.Errorf("page 2: NextCursor = %q, want empty (last page)", page2.NextCursor)
	}
}

// TestListSessions_SortDefaultUnchanged guards the backward-compat promise
// for the sort extension: omitting `sort` and `direction` keeps the
// `created_at DESC` shape that consumers relied on pre-#8.
func TestListSessions_SortDefaultUnchanged(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 || resp.Items[0].ID != "sess_b" || resp.Items[1].ID != "sess_a" {
		t.Errorf("default order = %v, want [sess_b sess_a] (created_at DESC)", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
}

func TestListSessions_SortByCreatedAtAsc(t *testing.T) {
	// Direction flip with the same key — oldest first.
	w := do(t, newTestHandler(t), "/sessions?sort=created_at&direction=asc")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" || resp.Items[1].ID != "sess_b" {
		t.Errorf("ASC order = %v, want [sess_a sess_b]", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
}

func TestListSessions_SortByLLMCallCount(t *testing.T) {
	// sess_a: 2 llm_response. sess_b: 1. DESC ⇒ sess_a first.
	w := do(t, newTestHandler(t), "/sessions?sort=llm_call_count&direction=desc")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" || resp.Items[1].ID != "sess_b" {
		t.Errorf("DESC by llm_call_count = %v, want [sess_a sess_b]", []string{resp.Items[0].ID, resp.Items[1].ID})
	}

	w = do(t, newTestHandler(t), "/sessions?sort=llm_call_count&direction=asc")
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_b" || resp.Items[1].ID != "sess_a" {
		t.Errorf("ASC by llm_call_count = %v, want [sess_b sess_a]", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
}

func TestListSessions_SortByToolCallCount(t *testing.T) {
	// sess_b: 1 tool_call_result. sess_a: 0. DESC ⇒ sess_b first.
	w := do(t, newTestHandler(t), "/sessions?sort=tool_call_count&direction=desc")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_b" || resp.Items[1].ID != "sess_a" {
		t.Errorf("DESC by tool_call_count = %v, want [sess_b sess_a]", []string{resp.Items[0].ID, resp.Items[1].ID})
	}
}

func TestListSessions_SortByTokensInTotal(t *testing.T) {
	// sess_a: tokens_in=3000. sess_b: 500. DESC ⇒ sess_a first.
	w := do(t, newTestHandler(t), "/sessions?sort=tokens_in_total&direction=desc")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" {
		t.Errorf("DESC by tokens_in_total: items[0] = %q, want sess_a", resp.Items[0].ID)
	}
}

func TestListSessions_SortByTokensOutTotal(t *testing.T) {
	// sess_a: tokens_out=1500. sess_b: 250.
	w := do(t, newTestHandler(t), "/sessions?sort=tokens_out_total&direction=desc")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" {
		t.Errorf("DESC by tokens_out_total: items[0] = %q, want sess_a", resp.Items[0].ID)
	}
}

func TestListSessions_SortByCostTotal(t *testing.T) {
	// sess_a cost = 0.0075 + 0.015 = 0.0225. sess_b cost = 0.00125 + 0.0025 = 0.00375.
	w := do(t, newTestHandler(t), "/sessions?sort=cost_total&direction=desc")
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" {
		t.Errorf("DESC by cost_total: items[0] = %q, want sess_a", resp.Items[0].ID)
	}
	w = do(t, newTestHandler(t), "/sessions?sort=cost_total&direction=asc")
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_b" {
		t.Errorf("ASC by cost_total: items[0] = %q, want sess_b", resp.Items[0].ID)
	}
}

// TestListSessions_SortCursorWalk_Aggregate covers the HAVING-clause branch
// of cursor pagination — without it, the cursor predicate would land in
// WHERE and the boundary value (which only exists after GROUP BY) would
// not match anything, returning an empty second page.
func TestListSessions_SortCursorWalk_Aggregate(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/sessions?sort=cost_total&direction=desc&limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if len(page1.Items) != 1 || page1.Items[0].ID != "sess_a" {
		t.Fatalf("page 1 = %+v, want one row sess_a", page1.Items)
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor required (more rows remain)")
	}
	w2 := do(t, h, "/sessions?sort=cost_total&direction=desc&limit=1&cursor="+page1.NextCursor)
	var page2 SessionListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if len(page2.Items) != 1 || page2.Items[0].ID != "sess_b" {
		t.Fatalf("page 2 = %+v, want one row sess_b", page2.Items)
	}
	if page2.NextCursor != "" {
		t.Errorf("page 2: NextCursor = %q, want empty (last page)", page2.NextCursor)
	}
}

func TestListSessions_SortCursorWalk_AscAggregate(t *testing.T) {
	// ASC direction also pages — checks the `>` arm of keysetCmp.
	h := newTestHandler(t)
	w := do(t, h, "/sessions?sort=tokens_in_total&direction=asc&limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if page1.Items[0].ID != "sess_b" {
		t.Fatalf("page 1 = %q, want sess_b (smallest tokens_in first)", page1.Items[0].ID)
	}
	w2 := do(t, h, "/sessions?sort=tokens_in_total&direction=asc&limit=1&cursor="+page1.NextCursor)
	var page2 SessionListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if page2.Items[0].ID != "sess_a" {
		t.Fatalf("page 2 = %q, want sess_a", page2.Items[0].ID)
	}
}

func TestListSessions_SortCursorMismatch(t *testing.T) {
	// Mint a cursor under cost_total, replay it under created_at —
	// must 400 cleanly rather than walking a meaningless keyset.
	h := newTestHandler(t)
	w := do(t, h, "/sessions?sort=cost_total&direction=desc&limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if page1.NextCursor == "" {
		t.Fatal("setup: no cursor minted")
	}
	w2 := do(t, h, "/sessions?sort=created_at&direction=desc&limit=1&cursor="+page1.NextCursor)
	if w2.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (cursor sort mismatch); body=%s", w2.Code, w2.Body.String())
	}
	if !strings.Contains(w2.Body.String(), "cursor") {
		t.Errorf("error body missing 'cursor' for debugging: %s", w2.Body.String())
	}
}

func TestListSessions_SortInvalidKey(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?sort=session_id")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (unsupported sort key)", w.Code)
	}
}

func TestListSessions_DirectionInvalid(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?direction=sideways")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (unsupported direction)", w.Code)
	}
}

// TestListSessions_LegacyTwoFieldCursor verifies the back-compat decode path:
// a cursor minted before the sort extension was introduced (just `ts|id`)
// must still walk under the default sort so in-flight cursors survive a
// deploy.
func TestListSessions_LegacyTwoFieldCursor(t *testing.T) {
	// Hand-craft the legacy form: sess_b's created_at | sess_b. Walking
	// under the default sort (created_at DESC) from this anchor should
	// land on sess_a (the row strictly less than sess_b's timestamp).
	legacyRaw := "2024-02-01T10:00:00Z|sess_b"
	legacy := base64.RawURLEncoding.EncodeToString([]byte(legacyRaw))
	w := do(t, newTestHandler(t), "/sessions?cursor="+legacy)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_a" {
		t.Errorf("legacy cursor walk = %+v, want [sess_a]", resp.Items)
	}
}

// TestListSessions_SortCursorWalk_LLMCallCount + _ToolCallCount fill the
// matrix gap: the cost_total and tokens_in_total walks already exercise
// the HAVING branch, but the per-event-type COUNT-CASE sort expressions
// (sortKeyLLMCallCount / sortKeyToolCallCount) build a different SQL
// shape and merit their own walks.
func TestListSessions_SortCursorWalk_LLMCallCount(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/sessions?sort=llm_call_count&direction=desc&limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if len(page1.Items) != 1 || page1.Items[0].ID != "sess_a" {
		t.Fatalf("page 1 = %+v, want sess_a (2 llm_responses)", page1.Items)
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor required")
	}
	w2 := do(t, h, "/sessions?sort=llm_call_count&direction=desc&limit=1&cursor="+page1.NextCursor)
	var page2 SessionListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if len(page2.Items) != 1 || page2.Items[0].ID != "sess_b" {
		t.Fatalf("page 2 = %+v, want sess_b", page2.Items)
	}
}

func TestListSessions_SortCursorWalk_ToolCallCount(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/sessions?sort=tool_call_count&direction=desc&limit=1")
	var page1 SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if len(page1.Items) != 1 || page1.Items[0].ID != "sess_b" {
		t.Fatalf("page 1 = %+v, want sess_b (1 tool_call_result)", page1.Items)
	}
	w2 := do(t, h, "/sessions?sort=tool_call_count&direction=desc&limit=1&cursor="+page1.NextCursor)
	var page2 SessionListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if len(page2.Items) != 1 || page2.Items[0].ID != "sess_a" {
		t.Fatalf("page 2 = %+v, want sess_a (0 tool_call_results)", page2.Items)
	}
}

// TestListSessions_SortWithFilter confirms filters still narrow under a
// non-default sort — without that guarantee the cost-by-tenant analytics
// query would silently drop the entity scope.
func TestListSessions_SortWithFilter(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?sort=cost_total&direction=desc&entity_id=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_a" {
		t.Errorf("Items = %+v, want only sess_a (entity_id filter under cost_total sort)", resp.Items)
	}
	// Totals must also reflect the filter — the user_1-scoped tile would
	// otherwise show the global cost.
	if resp.Totals.SessionCount != 1 {
		t.Errorf("Totals.SessionCount = %d, want 1", resp.Totals.SessionCount)
	}
}

// TestListSessions_LegacyCursorRejectedUnderNonDefaultSort pins the
// validation: a pre-#8 cursor (defaultSessionSort embedded) replayed
// against a non-default sort returns 400 cursor sort mismatch instead of
// silently walking a meaningless keyset under the new sort's space.
func TestListSessions_LegacyCursorRejectedUnderNonDefaultSort(t *testing.T) {
	legacyRaw := "2024-02-01T10:00:00Z|sess_b"
	legacy := base64.RawURLEncoding.EncodeToString([]byte(legacyRaw))
	w := do(t, newTestHandler(t), "/sessions?sort=cost_total&direction=desc&cursor="+legacy)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (legacy cursor mismatch); body = %s", w.Code, w.Body.String())
	}
}

// TestListSessions_EmptyFilterMatch is the zero-row contract: filter
// that matches nothing returns Items=[] and Totals zeroed out. The
// upstream guarantees this via COALESCE(..., 0) on every SUM/COUNT;
// a regression here would surface as null fields on the API.
func TestListSessions_EmptyFilterMatch(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?entity_id=ghost_user")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 {
		t.Errorf("Items = %d, want 0 (no match)", len(resp.Items))
	}
	if resp.Totals.SessionCount != 0 {
		t.Errorf("Totals.SessionCount = %d, want 0", resp.Totals.SessionCount)
	}
	if resp.Totals.TokensInTotal != 0 || resp.Totals.CostInputTotal != 0 {
		t.Errorf("Totals not zeroed: %+v", resp.Totals)
	}
	if resp.NextCursor != "" {
		t.Errorf("NextCursor = %q, want empty for empty page", resp.NextCursor)
	}
}

func TestListSessions_IncludeEntityIDs(t *testing.T) {
	// Single included entity_id narrows rows AND totals to that actor —
	// the multi-actor symmetric counterpart of exclude_entity_ids. With
	// one value the behaviour must match the singular entity_id form.
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_a" {
		t.Fatalf("Items = %+v, want exactly sess_a", resp.Items)
	}
	if resp.Totals.SessionCount != 1 {
		t.Errorf("Totals.SessionCount = %d, want 1", resp.Totals.SessionCount)
	}
	if resp.Totals.LLMCallCount != 2 {
		t.Errorf("Totals.LLMCallCount = %d, want 2 (sess_a's two llm_responses)", resp.Totals.LLMCallCount)
	}
	if resp.Totals.TokensInTotal != 3000 {
		t.Errorf("Totals.TokensInTotal = %d, want 3000", resp.Totals.TokensInTotal)
	}
}

func TestListSessions_IncludeEntityIDs_MultipleAndWhitespace(t *testing.T) {
	// Both seeded entities → all sessions back; whitespace/empty tokens
	// trimmed identically to exclude_entity_ids so callers don't sanitise
	// client-side.
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids=%20user_1%20,%20,user_2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 || resp.Totals.SessionCount != 2 {
		t.Errorf("got %d items / SessionCount=%d, want 2/2", len(resp.Items), resp.Totals.SessionCount)
	}
}

func TestListSessions_IncludeEntityIDs_EmptyParamIsNoop(t *testing.T) {
	// Empty value must not collapse the set to zero rows; default contract
	// (return all sessions) has to stay backward compatible.
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids=")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 || resp.Totals.SessionCount != 2 {
		t.Errorf("got %d items / SessionCount=%d, want 2/2", len(resp.Items), resp.Totals.SessionCount)
	}
}

func TestListSessions_IncludeEntityIDs_UnknownIsEmpty(t *testing.T) {
	// Unknown id matches nothing — same shape as a singular entity_id that
	// finds no rows: Items=[] and Totals zeroed by COALESCE.
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids=ghost_user")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 || resp.Totals.SessionCount != 0 {
		t.Errorf("Items=%d / SessionCount=%d, want 0/0", len(resp.Items), resp.Totals.SessionCount)
	}
}

func TestListSessions_IncludeEntityIDs_OverCap(t *testing.T) {
	// Symmetric cap with exclude_entity_ids — 400 instead of silently
	// truncating, so the caller learns when they overshoot.
	var b strings.Builder
	for i := 0; i < maxEntityIDList+1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("u%d", i))
	}
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids="+b.String())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (over cap); body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "include_entity_ids") {
		t.Errorf("error body missing param name for debugging: %s", w.Body.String())
	}
}

func TestListSessions_IncludeAndExcludeCombine(t *testing.T) {
	// include_entity_ids and exclude_entity_ids AND together — include
	// narrows the candidate set, exclude removes from what remains. Both
	// users included, sess_a excluded → only sess_b visible, totals match.
	w := do(t, newTestHandler(t), "/sessions?include_entity_ids=user_1,user_2&exclude_entity_ids=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_b" {
		t.Fatalf("Items = %+v, want exactly sess_b", resp.Items)
	}
	if resp.Totals.SessionCount != 1 || resp.Totals.LLMCallCount != 1 {
		t.Errorf("Totals = %+v, want SessionCount=1 / LLMCallCount=1", resp.Totals)
	}
}

func TestListSessions_IncludeWithSingularEntityID(t *testing.T) {
	// Singular entity_id is an equality predicate, include_entity_ids is
	// an IN-list — when both are set they AND together. The intersection
	// narrows: `entity_id=user_1 AND s.entity_id IN ('user_1','user_2')`
	// resolves to just sess_a (user_1's row), not both rows.
	w := do(t, newTestHandler(t), "/sessions?entity_id=user_1&include_entity_ids=user_1,user_2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_a" {
		t.Fatalf("Items = %+v, want exactly sess_a (intersection of entity_id and include list)", resp.Items)
	}
	if resp.Totals.SessionCount != 1 {
		t.Errorf("Totals.SessionCount = %d, want 1", resp.Totals.SessionCount)
	}
}

func TestListSessions_ExcludeEntityIDs(t *testing.T) {
	// Single excluded entity_id removes its rows AND its contribution to
	// totals — the two must always agree (visible rows sum to totals),
	// otherwise the review UI shows inconsistent numbers.
	w := do(t, newTestHandler(t), "/sessions?exclude_entity_ids=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].ID != "sess_b" {
		t.Fatalf("Items = %+v, want exactly sess_b", resp.Items)
	}
	if resp.Totals.SessionCount != 1 {
		t.Errorf("Totals.SessionCount = %d, want 1", resp.Totals.SessionCount)
	}
	if resp.Totals.LLMCallCount != 1 {
		t.Errorf("Totals.LLMCallCount = %d, want 1 (only sess_b's llm_response)", resp.Totals.LLMCallCount)
	}
	if resp.Totals.TokensInTotal != 500 {
		t.Errorf("Totals.TokensInTotal = %d, want 500 (sess_a's 3000 excluded)", resp.Totals.TokensInTotal)
	}
	if resp.Totals.CostInputTotal != 0.00125 {
		t.Errorf("Totals.CostInputTotal = %v, want 0.00125", resp.Totals.CostInputTotal)
	}
}

func TestListSessions_ExcludeEntityIDs_MultipleAndWhitespace(t *testing.T) {
	// Comma-separated list; whitespace and empty tokens are trimmed —
	// callers can pass " user_1 , ,user_2" without sanitising client-side.
	w := do(t, newTestHandler(t), "/sessions?exclude_entity_ids=%20user_1%20,%20,user_2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 {
		t.Errorf("Items = %d, want 0 (both entities excluded)", len(resp.Items))
	}
	if resp.Totals.SessionCount != 0 || resp.Totals.LLMCallCount != 0 || resp.Totals.CostInputTotal != 0 {
		t.Errorf("Totals not zeroed: %+v", resp.Totals)
	}
}

func TestListSessions_ExcludeEntityIDs_EmptyParamIsNoop(t *testing.T) {
	// Empty value must not flip the filter into "exclude everything"; the
	// default contract (return all sessions) has to stay backward compatible.
	w := do(t, newTestHandler(t), "/sessions?exclude_entity_ids=")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 || resp.Totals.SessionCount != 2 {
		t.Errorf("got %d items / SessionCount=%d, want 2/2", len(resp.Items), resp.Totals.SessionCount)
	}
}

func TestListSessions_ExcludeEntityIDs_OverCap(t *testing.T) {
	// Pathological 10k-id list would otherwise build a 10k-placeholder
	// NOT IN and trip SQLite's ~999 bind ceiling. Cap is enforced server-
	// side as 400 so the caller learns rather than getting truncated
	// silently — silent truncation would let through ids the requester
	// meant to exclude.
	var b strings.Builder
	for i := 0; i < maxEntityIDList+1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("u%d", i))
	}
	w := do(t, newTestHandler(t), "/sessions?exclude_entity_ids="+b.String())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (over cap); body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "exclude_entity_ids") {
		t.Errorf("error body missing param name for debugging: %s", w.Body.String())
	}
}

func TestListSessions_BadCursor(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?cursor=not-base64")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestListSessions_BadTimestamp(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?since=yesterday")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestGetSession_FullDetail(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)
	if len(s.Messages) != 2 {
		t.Errorf("got %d messages, want 2", len(s.Messages))
	}
	if len(s.Events) != 2 {
		t.Errorf("got %d events, want 2", len(s.Events))
	}
	if s.Stats.LLMCallCount != 2 {
		t.Errorf("Stats.LLMCallCount = %d, want 2", s.Stats.LLMCallCount)
	}
	// Payload is inlined as raw JSON, not double-escaped.
	if len(s.Events[0].Payload) == 0 || s.Events[0].Payload[0] != '{' {
		t.Errorf("first event payload not raw JSON: %s", string(s.Events[0].Payload))
	}
}

func TestListSessions_TitleRoundTrip(t *testing.T) {
	// sess_a has a populated title; sess_b has NULL — the response must
	// echo the populated string and omit the field for the NULL row so
	// pre-generation sessions don't render an empty bubble.
	w := do(t, newTestHandler(t), "/sessions")
	mustStatus(t, w, http.StatusOK)
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 {
		t.Fatalf("got %d sessions, want 2", len(resp.Items))
	}
	// Default sort is created_at DESC → sess_b (Feb) first, sess_a (Jan) second.
	if resp.Items[0].ID != "sess_b" || resp.Items[0].Title != "" {
		t.Errorf("sess_b.Title = %q, want empty (NULL in DB)", resp.Items[0].Title)
	}
	if resp.Items[1].ID != "sess_a" || resp.Items[1].Title != "First chat title" {
		t.Errorf("sess_a.Title = %q, want %q", resp.Items[1].Title, "First chat title")
	}
	// JSON envelope: title field absent when empty (omitempty on the
	// struct tag). Walk the raw bytes so this catches accidental "":"
	// renderings if someone removes omitempty later.
	if strings.Contains(w.Body.String(), `"id":"sess_b"`) &&
		strings.Contains(w.Body.String(), `"id":"sess_b","entity_id":"user_2","group_id":"group_y","title":""`) {
		t.Errorf("empty title rendered as \"\" instead of being omitted: %s", w.Body.String())
	}
}

func TestGetSession_HiddenMessagesAreReaderAware(t *testing.T) {
	h := newTestHandler(t)
	// sess_a starts with 2 visible messages; add one hidden system-injected turn.
	if _, err := h.db.Exec(
		`INSERT INTO messages (session_id, seq, role, content, visibility, created_at)
		 VALUES ('sess_a', 3, 'user', '[system] job done', 'hidden', '2024-01-01T10:00:02Z')`); err != nil {
		t.Fatalf("insert hidden message: %v", err)
	}

	// Customer view (default): the hidden turn is dropped.
	w := do(t, h, "/sessions/sess_a")
	mustStatus(t, w, http.StatusOK)
	var customer SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &customer)
	if len(customer.Messages) != 2 {
		t.Errorf("customer view: got %d messages, want 2 (hidden dropped)", len(customer.Messages))
	}

	// Staff view: include_hidden=true returns the hidden turn for debugging.
	w2 := do(t, h, "/sessions/sess_a?include_hidden=true")
	mustStatus(t, w2, http.StatusOK)
	var staff SessionDetail
	mustUnmarshal(t, w2.Body.Bytes(), &staff)
	if len(staff.Messages) != 3 {
		t.Errorf("staff view: got %d messages, want 3 (hidden included)", len(staff.Messages))
	}
}

func TestGetSession_TitleRoundTrip(t *testing.T) {
	// Mirrors the list test on the detail endpoint so consumers can pick
	// either entry point.
	w := do(t, newTestHandler(t), "/sessions/sess_a")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)
	if s.Title != "First chat title" {
		t.Errorf("Title = %q, want %q", s.Title, "First chat title")
	}

	w2 := do(t, newTestHandler(t), "/sessions/sess_b")
	mustStatus(t, w2, http.StatusOK)
	var s2 SessionDetail
	mustUnmarshal(t, w2.Body.Bytes(), &s2)
	if s2.Title != "" {
		t.Errorf("sess_b Title = %q, want empty", s2.Title)
	}
}

func TestListSessions_SortByUpdatedAtDesc(t *testing.T) {
	// updated_at sort is the dropdown-ordering key for the chat-widget
	// session picker (most recently active first). seed: sess_b
	// updated 2024-02-01, sess_a updated 2024-01-01 → DESC = b, a.
	w := do(t, newTestHandler(t), "/sessions?sort=updated_at")
	mustStatus(t, w, http.StatusOK)
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 {
		t.Fatalf("got %d sessions, want 2", len(resp.Items))
	}
	if resp.Items[0].ID != "sess_b" || resp.Items[1].ID != "sess_a" {
		t.Errorf("order = [%s, %s], want [sess_b, sess_a]", resp.Items[0].ID, resp.Items[1].ID)
	}
}

func TestListSessions_SortByUpdatedAtAsc(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions?sort=updated_at&direction=asc")
	mustStatus(t, w, http.StatusOK)
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp.Items[0].ID != "sess_a" || resp.Items[1].ID != "sess_b" {
		t.Errorf("order = [%s, %s], want [sess_a, sess_b]", resp.Items[0].ID, resp.Items[1].ID)
	}
}

func TestGetSession_NotFound(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/does_not_exist")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// TestGetSession_ToolCallsPassthrough is the regression guard for the
// messages-serializer fix that started shipping native_tool_calls_raw
// to consumers. Before this fix /sessions/{id} rendered tool-calling
// assistant turns as `content:""` with no tool_calls field, forcing the
// chat-bubble consumer to reconstruct the call from the events stream.
//
// Pairing contract under test: n-th assistant message ↔ n-th
// llm_response event, by ordinal. Non-assistant rows (user, tool) and
// text-only assistant rows must not gain a tool_calls field — the
// passthrough is strictly additive.
func TestGetSession_ToolCallsPassthrough(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_tc','tool-call session',NULL,'gpt-4o','{}','user_3','group_z','2024-03-01T10:00:00Z','2024-03-01T10:30:00Z')`)

	// user → assistant tool-call-only → tool → assistant text answer.
	// The first assistant row has empty content (the LLM only emitted
	// tool_calls); the second carries the final text. Both should pair
	// with the two llm_response events in order.
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_tc',1,'user','show me items','2024-03-01T10:00:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_tc',2,'assistant','','2024-03-01T10:00:01Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_tc',3,'tool','{"items":[]}','2024-03-01T10:00:02Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_tc',4,'assistant','No items found.','2024-03-01T10:00:03Z')`)

	toolCallsPayload := `{"v":1,"raw_content_excerpt":"","tokens_in":50,"tokens_out":20,"cost_input":0,"cost_output":0,"native_tool_calls_raw":[{"id":"call_1","type":"function","function":{"name":"list-items","arguments":"{}"}}],"finish_reason":"tool_calls"}`
	textOnlyPayload := `{"v":1,"raw_content_excerpt":"No items","tokens_in":80,"tokens_out":15,"cost_input":0,"cost_output":0,"finish_reason":"stop"}`
	exec(`INSERT INTO session_events VALUES ('evt_tc1','sess_tc',1,'2024-03-01T10:00:00Z','llm_response',NULL,100,?,'2024-03-01T10:00:00Z')`,
		toolCallsPayload)
	exec(`INSERT INTO session_events VALUES ('evt_tc2','sess_tc',2,'2024-03-01T10:00:02Z','tool_call_result','evt_tc1',50,'{"v":1}','2024-03-01T10:00:02Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_tc3','sess_tc',3,'2024-03-01T10:00:03Z','llm_response',NULL,80,?,'2024-03-01T10:00:03Z')`,
		textOnlyPayload)

	w := do(t, h, "/sessions/sess_tc")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 4 {
		t.Fatalf("got %d messages, want 4", len(s.Messages))
	}
	if len(s.Messages[0].ToolCalls) != 0 {
		t.Errorf("user row tool_calls = %s, want absent", s.Messages[0].ToolCalls)
	}
	tc := string(s.Messages[1].ToolCalls)
	if !strings.Contains(tc, `"list-items"`) || !strings.Contains(tc, `"call_1"`) {
		t.Errorf("tool-call assistant row tool_calls = %s, want raw native_tool_calls_raw passthrough", tc)
	}
	if len(s.Messages[2].ToolCalls) != 0 {
		t.Errorf("tool row tool_calls = %s, want absent", s.Messages[2].ToolCalls)
	}
	if len(s.Messages[3].ToolCalls) != 0 {
		t.Errorf("text-only assistant row tool_calls = %s, want absent (payload has no native_tool_calls_raw)", s.Messages[3].ToolCalls)
	}

	// omitempty contract: the JSON wire form has the key exactly once,
	// on the tool-call assistant turn. Match `"tool_calls":` rather than
	// the bare token — the event payload here contains `finish_reason:
	// "tool_calls"` as a string value, which would otherwise inflate
	// the count.
	if n := strings.Count(w.Body.String(), `"tool_calls":`); n != 1 {
		t.Errorf("tool_calls JSON key appears %d times, want 1 (only on the tool-call assistant turn); body = %s", n, w.Body.String())
	}
}

// TestGetSession_MessageMetadataPassthrough pins the per-message metadata
// column (opentalon-core migration 013): a tool-confirmation prompt row and its
// reply row carry a raw JSON metadata map through to the API response, while a
// plain turn (NULL metadata) omits the key entirely — the byte-identical
// contract that lets the chat widget rebuild the Approve/Reject UI after reload.
func TestGetSession_MessageMetadataPassthrough(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_md','md session',NULL,'gpt-4o','{}','user_5','group_z','2024-08-01T10:00:00Z','2024-08-01T10:30:00Z')`)
	// Plain user turn (NULL metadata) → key omitted.
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_md',1,'user','delete 3 items','2024-08-01T10:00:00Z')`)
	// Assistant confirmation prompt carries the tool-confirmation marker.
	exec(`INSERT INTO messages (session_id, seq, role, content, metadata, created_at) VALUES ('sess_md',2,'assistant','Proceed?','{"prompt_type":"tool_confirmation","tool_call_id":"call_9","options":"approve,reject"}','2024-08-01T10:00:01Z')`)
	// User reply carries the confirmation_response marker.
	exec(`INSERT INTO messages (session_id, seq, role, content, metadata, created_at) VALUES ('sess_md',3,'user','Approve','{"prompt_type":"confirmation_response","action":"approve"}','2024-08-01T10:00:02Z')`)

	w := do(t, h, "/sessions/sess_md")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 3 {
		t.Fatalf("got %d messages, want 3", len(s.Messages))
	}
	if len(s.Messages[0].Metadata) != 0 {
		t.Errorf("plain user row metadata = %s, want absent", s.Messages[0].Metadata)
	}
	var prompt map[string]string
	if err := json.Unmarshal(s.Messages[1].Metadata, &prompt); err != nil {
		t.Fatalf("prompt row metadata not valid JSON: %v (raw %s)", err, s.Messages[1].Metadata)
	}
	if prompt["prompt_type"] != "tool_confirmation" || prompt["tool_call_id"] != "call_9" {
		t.Errorf("prompt row metadata mismatch: %+v", prompt)
	}
	var reply map[string]string
	if err := json.Unmarshal(s.Messages[2].Metadata, &reply); err != nil {
		t.Fatalf("reply row metadata not valid JSON: %v", err)
	}
	if reply["prompt_type"] != "confirmation_response" || reply["action"] != "approve" {
		t.Errorf("reply row metadata mismatch: %+v", reply)
	}

	// omitempty contract: the JSON key appears exactly on the two rows that
	// carry metadata. Session-level metadata is '{}' → omitted, so it does not
	// inflate the count.
	if n := strings.Count(w.Body.String(), `"metadata":`); n != 2 {
		t.Errorf("metadata JSON key appears %d times, want 2; body = %s", n, w.Body.String())
	}
}

// TestGetSession_MalformedMessageMetadataDropped guards the read path: a row
// whose metadata column holds non-JSON bytes must be dropped (key omitted), not
// inlined — otherwise one bad row would produce an invalid response body for
// the whole session.
func TestGetSession_MalformedMessageMetadataDropped(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	exec(`INSERT INTO sessions VALUES ('sess_bad','bad md',NULL,'gpt-4o','{}','user_6','group_z','2024-08-02T10:00:00Z','2024-08-02T10:30:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, metadata, created_at) VALUES ('sess_bad',1,'assistant','x','not json','2024-08-02T10:00:00Z')`)

	w := do(t, h, "/sessions/sess_bad")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)
	if len(s.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(s.Messages))
	}
	if len(s.Messages[0].Metadata) != 0 {
		t.Errorf("malformed metadata should be dropped, got %s", s.Messages[0].Metadata)
	}
	if strings.Contains(w.Body.String(), `"metadata":`) {
		t.Errorf("malformed metadata leaked into body: %s", w.Body.String())
	}
}

// TestGetSession_EmptyToolCallsOmitted covers the two "no tool calls
// were actually made" payload shapes — explicit null and explicit empty
// array. Both must be treated as absent so the bubble UI doesn't render
// "assistant invoked 0 tools" affordance on a plain text turn.
func TestGetSession_EmptyToolCallsOmitted(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_empty_tc','empty-tc',NULL,'gpt-4o','{}','user_4','group_z','2024-03-02T10:00:00Z','2024-03-02T10:30:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_empty_tc',1,'user','hi','2024-03-02T10:00:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_empty_tc',2,'assistant','hello','2024-03-02T10:00:01Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_empty_tc',3,'assistant','again','2024-03-02T10:00:02Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_empty1','sess_empty_tc',1,'2024-03-02T10:00:00Z','llm_response',NULL,100,'{"v":1,"native_tool_calls_raw":null}','2024-03-02T10:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_empty2','sess_empty_tc',2,'2024-03-02T10:00:02Z','llm_response',NULL,100,'{"v":1,"native_tool_calls_raw":[]}','2024-03-02T10:00:02Z')`)

	w := do(t, h, "/sessions/sess_empty_tc")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	for i, m := range s.Messages {
		if len(m.ToolCalls) != 0 {
			t.Errorf("messages[%d] (role=%s) tool_calls = %s, want absent for null/[] payload", i, m.Role, m.ToolCalls)
		}
	}
	if n := strings.Count(w.Body.String(), `"tool_calls":`); n != 0 {
		t.Errorf("tool_calls JSON key appears %d times, want 0; body = %s", n, w.Body.String())
	}
}

// TestGetSession_SynthesisesErrorRowFromLLMError covers the failed-turn
// case: a session where the LLM call never produced a response (timeout,
// connect refused, 5xx). The orchestrator emits `llm_error` into
// session_events but writes nothing to the messages table — left
// uncorrected, the diagnostic page would show the conversation ending
// after the user question with no acknowledgement. The synthetic
// `role:"error"` row makes the failure visible without touching the
// underlying messages table (which the orchestrator reads back into the
// next turn's LLM context).
func TestGetSession_SynthesisesErrorRowFromLLMError(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_err','failed turn',NULL,'gpt-4o','{}','user_4','group_z','2024-04-01T10:00:00Z','2024-04-01T10:00:05Z')`)

	// Only the user message exists — the LLM call errored before any
	// assistant row could be written.
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_err',1,'user','Wieviele Items habe ich?','2024-04-01T10:00:00Z')`)

	// Full turn-startup events (knowledge_retrieval, turn_start,
	// llm_request) are omitted from this seed — they're irrelevant to
	// the merge. The pairing key is the `llm_error` event itself plus
	// its timestamp.
	exec(`INSERT INTO session_events VALUES ('evt_err1','sess_err',1,'2024-04-01T10:00:00.500Z','llm_request',NULL,0,'{}','2024-04-01T10:00:00.500Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_err2','sess_err',2,'2024-04-01T10:00:02Z','llm_error',NULL,0,'{"v":1,"phase":"chat.transport","response_body_excerpt":"context deadline exceeded"}','2024-04-01T10:00:02Z')`)

	w := do(t, h, "/sessions/sess_err")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 2 {
		t.Fatalf("got %d messages, want 2 (user + synthetic error); body = %s", len(s.Messages), w.Body.String())
	}
	if s.Messages[0].Role != "user" {
		t.Errorf("messages[0].Role = %q, want user", s.Messages[0].Role)
	}
	if s.Messages[1].Role != "error" {
		t.Errorf("messages[1].Role = %q, want error", s.Messages[1].Role)
	}
	if !strings.Contains(s.Messages[1].Content, "context deadline exceeded") {
		t.Errorf("messages[1].Content = %q, want it to inline the response_body_excerpt", s.Messages[1].Content)
	}
	if s.Messages[1].CreatedAt != "2024-04-01T10:00:02Z" {
		t.Errorf("messages[1].CreatedAt = %q, want it to mirror the llm_error ts", s.Messages[1].CreatedAt)
	}
}

// TestGetSession_DoesNotInjectWhenSessionIsClean is the regression guard
// that the existing TestGetSession_FullDetail fixture (which has zero
// llm_error events) keeps returning byte-identical 2-message output.
// Otherwise this PR would silently grow the response on every clean
// session.
func TestGetSession_DoesNotInjectWhenSessionIsClean(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 2 {
		t.Fatalf("got %d messages, want 2 (no synthesis on clean session); body = %s", len(s.Messages), w.Body.String())
	}
	for i, m := range s.Messages {
		if m.Role == "error" {
			t.Errorf("messages[%d] is role=error on a clean session; full = %+v", i, s.Messages)
		}
	}
}

// TestGetSession_ErrorRowInterleavedByTimestamp covers the multi-turn
// case: a session where the first turn succeeded normally, then a
// follow-up turn failed. The synthetic error row must land *after* the
// successful assistant message and the second user message — pairing is
// by timestamp, not by ordinal, so it survives any future event-order
// changes upstream.
func TestGetSession_ErrorRowInterleavedByTimestamp(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_mix','mixed session',NULL,'gpt-4o','{}','user_5','group_z','2024-05-01T10:00:00Z','2024-05-01T10:00:30Z')`)

	// Turn 1: user → assistant (successful).
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_mix',1,'user','hi','2024-05-01T10:00:00Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_mix',2,'assistant','hello','2024-05-01T10:00:01Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_mx1','sess_mix',1,'2024-05-01T10:00:01Z','llm_response',NULL,100,'{"v":1}','2024-05-01T10:00:01Z')`)

	// Turn 2: user → llm_error (failed, no assistant row).
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_mix',3,'user','tell me more','2024-05-01T10:00:10Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_mx2','sess_mix',2,'2024-05-01T10:00:12Z','llm_error',NULL,0,'{"v":1,"phase":"chat.transport","response_body_excerpt":"connection refused"}','2024-05-01T10:00:12Z')`)

	w := do(t, h, "/sessions/sess_mix")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 4 {
		t.Fatalf("got %d messages, want 4 (user, assistant, user, error); body = %s", len(s.Messages), w.Body.String())
	}
	wantRoles := []string{"user", "assistant", "user", "error"}
	for i, want := range wantRoles {
		if s.Messages[i].Role != want {
			t.Errorf("messages[%d].Role = %q, want %q (full sequence: %+v)", i, s.Messages[i].Role, want, rolesOf(s.Messages))
		}
	}
	if !strings.Contains(s.Messages[3].Content, "connection refused") {
		t.Errorf("messages[3].Content = %q, want it to inline the response_body_excerpt", s.Messages[3].Content)
	}
}

// TestGetSession_PrecisionMismatchOrdersErrorAfterMessage is the
// regression guard for the precision-mismatch bug. messages.created_at
// is written by Core's `sessions.AddMessage` at second precision
// (`time.Now().Format(time.RFC3339)`), while session_events.ts is
// microsecond precision. Within the same wall-clock second, a
// sub-second event ts (`...36.050Z`) lex-sorts BEFORE the unfractioned
// message ts (`...36Z`) because `.` (0x2E) < `Z` (0x5A) — which would
// place the synthetic error row BEFORE the user message that opened
// the failed turn. Verified live in opentalon_dev on 2026-05-18:
// the chat UI rendered the error row above the user message.
//
// The fix parses both stamps as time.Time and uses time.Before for
// chronological (not lexicographic) comparison.
func TestGetSession_PrecisionMismatchOrdersErrorAfterMessage(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	exec(`INSERT INTO sessions VALUES ('sess_prec','precision session',NULL,'gpt-4o','{}','user_7','group_z','2024-07-01T10:00:00Z','2024-07-01T10:00:05Z')`)

	// Real-world shape: user message at second precision, llm_error at
	// microsecond precision within the same wall-clock second. The
	// event actually happened ~50 ms AFTER the message was written —
	// the synthetic error row must land AFTER the user.
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_prec',1,'user','q','2024-07-01T10:00:01Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_pr1','sess_prec',1,'2024-07-01T10:00:01.050745Z','llm_error',NULL,0,'{"v":1,"phase":"chat.transport","response_body_excerpt":"dial tcp: connect refused"}','2024-07-01T10:00:01.050745Z')`)

	w := do(t, h, "/sessions/sess_prec")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	wantRoles := []string{"user", "error"}
	got := rolesOf(s.Messages)
	if len(s.Messages) != len(wantRoles) {
		t.Fatalf("got %d messages, want %d (sequence %v); body = %s", len(s.Messages), len(wantRoles), got, w.Body.String())
	}
	for i, want := range wantRoles {
		if s.Messages[i].Role != want {
			t.Errorf("messages[%d].Role = %q, want %q (full sequence: %v)", i, s.Messages[i].Role, want, got)
		}
	}
}

// TestGetSession_ErrorContentFallback covers payloads that don't carry
// a usable excerpt (oversized, malformed, or missing field). The
// synthetic row falls back to a static string so the diagnostic page
// still shows "this turn failed" without leaking pathological
// upstream blobs into the chat-bubble view.
func TestGetSession_ErrorContentFallback(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	exec(`INSERT INTO sessions VALUES ('sess_fb','fallback session',NULL,'gpt-4o','{}','user_6','group_z','2024-06-01T10:00:00Z','2024-06-01T10:00:05Z')`)
	exec(`INSERT INTO messages (session_id, seq, role, content, created_at) VALUES ('sess_fb',1,'user','q','2024-06-01T10:00:00Z')`)
	// Payload is valid JSON but has no response_body_excerpt — must fall back.
	exec(`INSERT INTO session_events VALUES ('evt_fb1','sess_fb',1,'2024-06-01T10:00:02Z','llm_error',NULL,0,'{"v":1,"phase":"chat.parse"}','2024-06-01T10:00:02Z')`)

	w := do(t, h, "/sessions/sess_fb")
	mustStatus(t, w, http.StatusOK)
	var s SessionDetail
	mustUnmarshal(t, w.Body.Bytes(), &s)

	if len(s.Messages) != 2 || s.Messages[1].Role != "error" {
		t.Fatalf("expected one synthetic error row; got %+v", rolesOf(s.Messages))
	}
	if s.Messages[1].Content != "[LLM error] The request could not be completed." {
		t.Errorf("messages[1].Content = %q, want static fallback (payload had no response_body_excerpt)", s.Messages[1].Content)
	}
}

// rolesOf is a small test helper that produces a compact role-only
// snapshot of a Messages slice for use in t.Errorf — easier to scan
// than printing the full Message struct on every diagnostic line.
func rolesOf(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Role
	}
	return out
}

func TestListEvents_FilterByType(t *testing.T) {
	w := do(t, newTestHandler(t), "/events?event_type=tool_call_result")
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].EventType != "tool_call_result" {
		t.Fatalf("got %+v, want one tool_call_result event", resp.Items)
	}
}

func TestListEvents_FilterByEntityRequiresJoin(t *testing.T) {
	w := do(t, newTestHandler(t), "/events?entity_id=user_1")
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 {
		t.Fatalf("got %d events, want 2 (both belong to user_1's sess_a)", len(resp.Items))
	}
	for _, e := range resp.Items {
		if e.SessionID != "sess_a" {
			t.Errorf("event %s session = %q, want sess_a (entity filter must scope correctly)", e.ID, e.SessionID)
		}
	}
}

// TestListEvents_CursorPagination mirrors the /sessions cursor walk for
// /events — same limit+1 probe, same composite (ts, id) cursor — so a
// regression in either site has its own test guard.
func TestListEvents_CursorPagination(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/events?limit=2")
	var page1 EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &page1)
	if len(page1.Items) != 2 {
		t.Fatalf("page 1: got %d items, want 2", len(page1.Items))
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1: NextCursor must be set (4 events total, page size 2)")
	}

	w2 := do(t, h, "/events?limit=2&cursor="+page1.NextCursor)
	var page2 EventListResponse
	mustUnmarshal(t, w2.Body.Bytes(), &page2)
	if len(page2.Items) != 2 {
		t.Fatalf("page 2: got %d items, want 2", len(page2.Items))
	}
	if page2.NextCursor != "" {
		t.Errorf("page 2: NextCursor = %q, want empty (last page)", page2.NextCursor)
	}
	// Pages must not overlap: union of IDs across both pages must equal all
	// 4 seeded events with no duplicates.
	seen := make(map[string]struct{}, 4)
	for _, e := range page1.Items {
		seen[e.ID] = struct{}{}
	}
	for _, e := range page2.Items {
		if _, dup := seen[e.ID]; dup {
			t.Errorf("event %q appeared on both pages — cursor strictly-less semantics broken", e.ID)
		}
		seen[e.ID] = struct{}{}
	}
	if len(seen) != 4 {
		t.Errorf("walked %d distinct events across pages, want 4", len(seen))
	}
}

func TestListEvents_IncludeEntityIDs(t *testing.T) {
	// /events listing honours include_entity_ids — forces the otherwise-
	// optional JOIN onto sessions, then applies the IN predicate. Without
	// the JOIN-forcing branch the filter would silently no-op when
	// entity_id/group_id are unset.
	w := do(t, newTestHandler(t), "/events?include_entity_ids=user_2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	// user_2 owns sess_b → 2 events; user_1's sess_a contributes nothing.
	if len(resp.Items) != 2 {
		t.Errorf("Items = %d, want 2 (only sess_b's events)", len(resp.Items))
	}
	for _, e := range resp.Items {
		if e.SessionID != "sess_b" {
			t.Errorf("event %s belongs to %s, want sess_b only", e.ID, e.SessionID)
		}
	}
}

func TestListEvents_ExcludeEntityIDs(t *testing.T) {
	// /events listing must honour exclude_entity_ids too — forces the
	// otherwise-optional JOIN onto sessions, then applies the same
	// NOT IN predicate as /sessions. Without the JOIN-forcing branch
	// the filter would silently no-op when entity_id/group_id are unset.
	w := do(t, newTestHandler(t), "/events?exclude_entity_ids=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	// sess_a's 2 llm_response events excluded, only sess_b's 2 events remain.
	if len(resp.Items) != 2 {
		t.Errorf("Items = %d, want 2 (sess_a's 2 events excluded)", len(resp.Items))
	}
	for _, e := range resp.Items {
		if e.SessionID != "sess_b" {
			t.Errorf("event %s belongs to %s, want sess_b only", e.ID, e.SessionID)
		}
	}
}

func TestListEvents_PayloadOmittedWhenRequested(t *testing.T) {
	w := do(t, newTestHandler(t), "/events?include_payload=false")
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	for _, e := range resp.Items {
		if len(e.Payload) > 0 {
			t.Errorf("event %s carried payload despite include_payload=false: %s", e.ID, string(e.Payload))
		}
	}
}

func TestEventsStats_CrossSessionTotals(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.SessionCount != 2 {
		t.Errorf("SessionCount = %d, want 2", stats.SessionCount)
	}
	if stats.EventCount != 4 {
		t.Errorf("EventCount = %d, want 4", stats.EventCount)
	}
	if stats.LLMCallCount != 3 {
		t.Errorf("LLMCallCount = %d, want 3", stats.LLMCallCount)
	}
	if stats.ToolCallCount != 1 {
		t.Errorf("ToolCallCount = %d, want 1", stats.ToolCallCount)
	}
	// 1000 + 2000 + 500 = 3500 tokens_in across the three llm_response payloads.
	if stats.TokensInTotal != 3500 {
		t.Errorf("TokensInTotal = %d, want 3500", stats.TokensInTotal)
	}
}

func TestEventsStats_FilteredByGroup(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_id=group_x")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.SessionCount != 1 {
		t.Errorf("SessionCount = %d, want 1 (only group_x)", stats.SessionCount)
	}
	if stats.LLMCallCount != 2 {
		t.Errorf("LLMCallCount = %d, want 2 (only sess_a)", stats.LLMCallCount)
	}
}

func TestEventsStats_IncludeEntityIDs(t *testing.T) {
	// /events/stats must apply include_entity_ids identically to /sessions
	// — same filter helper, same SQL. List/totals tile and stats
	// dashboard would disagree without that guarantee.
	w := do(t, newTestHandler(t), "/events/stats?include_entity_ids=user_2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.SessionCount != 1 {
		t.Errorf("SessionCount = %d, want 1 (only sess_b under user_2)", stats.SessionCount)
	}
	if stats.LLMCallCount != 1 {
		t.Errorf("LLMCallCount = %d, want 1", stats.LLMCallCount)
	}
	if stats.TokensInTotal != 500 {
		t.Errorf("TokensInTotal = %d, want 500", stats.TokensInTotal)
	}
}

func TestEventsStats_ExcludeEntityIDs(t *testing.T) {
	// /events/stats must apply exclude_entity_ids identically to /sessions
	// — same filter helper, same SQL. Without that guarantee a caller's
	// list/totals tile and stats dashboard would disagree.
	w := do(t, newTestHandler(t), "/events/stats?exclude_entity_ids=user_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.SessionCount != 1 {
		t.Errorf("SessionCount = %d, want 1 (sess_a excluded)", stats.SessionCount)
	}
	if stats.LLMCallCount != 1 {
		t.Errorf("LLMCallCount = %d, want 1 (only sess_b's llm_response)", stats.LLMCallCount)
	}
	if stats.TokensInTotal != 500 {
		t.Errorf("TokensInTotal = %d, want 500 (sess_a's 3000 excluded)", stats.TokensInTotal)
	}
}

// TestEventsStats_DefaultShapeUnchanged guards the backwards-compatibility
// promise of the group_by extension: omitting the new params yields the
// exact same response shape as before the change. by_event_type is
// absent from the JSON, not present-but-empty.
func TestEventsStats_DefaultShapeUnchanged(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "by_event_type") {
		t.Errorf("default response leaked by_event_type key: %s", w.Body.String())
	}
}

func TestEventsStats_GroupByEventType_CountsAndOrdering(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_by=event_type")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.ByEventType) != 2 {
		t.Fatalf("ByEventType len = %d, want 2 (llm_response + tool_call_result)", len(stats.ByEventType))
	}
	// count DESC, so llm_response (3) before tool_call_result (1).
	if stats.ByEventType[0].EventType != "llm_response" || stats.ByEventType[0].Count != 3 {
		t.Errorf("bucket[0] = %+v, want {llm_response, 3}", stats.ByEventType[0])
	}
	if stats.ByEventType[1].EventType != "tool_call_result" || stats.ByEventType[1].Count != 1 {
		t.Errorf("bucket[1] = %+v, want {tool_call_result, 1}", stats.ByEventType[1])
	}
	// Bucket sum must match top-level event_count — same JIT JOIN
	// underneath, so any drift means the filter set diverged.
	var bucketSum int64
	for _, b := range stats.ByEventType {
		bucketSum += b.Count
	}
	if bucketSum != stats.EventCount {
		t.Errorf("sum(bucket counts) = %d, want event_count = %d", bucketSum, stats.EventCount)
	}
	// Without sample_sessions, no sample_session_ids on any bucket.
	for _, b := range stats.ByEventType {
		if b.SampleSessionIDs != nil {
			t.Errorf("bucket %q leaked sample_session_ids without ?sample_sessions: %v",
				b.EventType, b.SampleSessionIDs)
		}
	}
}

// TestEventsStats_GroupBy_TiebreakerEventTypeAsc adds two extra event
// types with equal counts so the (event_type ASC) tiebreaker — not just
// the (count DESC) primary sort — is exercised.
func TestEventsStats_GroupBy_TiebreakerEventTypeAsc(t *testing.T) {
	h := newTestHandler(t)
	// Two more event types, each with one event — same count, distinct
	// types. event_type ASC means "retry" sorts before "zzz_aux".
	if _, err := h.db.Exec(
		`INSERT INTO session_events VALUES ('evt_extra1','sess_a',3,'2024-01-01T11:00:00Z','retry',NULL,10,'{}','2024-01-01T11:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.Exec(
		`INSERT INTO session_events VALUES ('evt_extra2','sess_b',3,'2024-02-01T11:00:00Z','zzz_aux',NULL,10,'{}','2024-02-01T11:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	w := do(t, h, "/events/stats?group_by=event_type")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.ByEventType) != 4 {
		t.Fatalf("ByEventType len = %d, want 4; got %+v", len(stats.ByEventType), stats.ByEventType)
	}
	// llm_response (3), tool_call_result (1), retry (1), zzz_aux (1).
	// Among the count-1 bucket: retry < tool_call_result < zzz_aux.
	want := []struct {
		t string
		c int64
	}{
		{"llm_response", 3},
		{"retry", 1},
		{"tool_call_result", 1},
		{"zzz_aux", 1},
	}
	for i, w := range want {
		if stats.ByEventType[i].EventType != w.t || stats.ByEventType[i].Count != w.c {
			t.Errorf("bucket[%d] = %+v, want {%s, %d}",
				i, stats.ByEventType[i], w.t, w.c)
		}
	}
}

func TestEventsStats_GroupBy_WithSampleSessions(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_by=event_type&sample_sessions=5")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.ByEventType) != 2 {
		t.Fatalf("ByEventType len = %d, want 2", len(stats.ByEventType))
	}
	// llm_response: both sessions touch it. Most-recent first means
	// sess_b (2024-02-01) before sess_a (2024-01-01).
	llm := stats.ByEventType[0]
	if llm.EventType != "llm_response" {
		t.Fatalf("bucket[0] = %q, want llm_response", llm.EventType)
	}
	wantSamples := []string{"sess_b", "sess_a"}
	if len(llm.SampleSessionIDs) != len(wantSamples) {
		t.Fatalf("llm_response sample_session_ids = %v, want %v", llm.SampleSessionIDs, wantSamples)
	}
	for i, want := range wantSamples {
		if llm.SampleSessionIDs[i] != want {
			t.Errorf("llm_response samples[%d] = %q, want %q (most-recent-first)", i, llm.SampleSessionIDs[i], want)
		}
	}
	// tool_call_result: only sess_b has one.
	tool := stats.ByEventType[1]
	if tool.EventType != "tool_call_result" {
		t.Fatalf("bucket[1] = %q, want tool_call_result", tool.EventType)
	}
	if len(tool.SampleSessionIDs) != 1 || tool.SampleSessionIDs[0] != "sess_b" {
		t.Errorf("tool_call_result samples = %v, want [sess_b]", tool.SampleSessionIDs)
	}
}

// TestEventsStats_GroupBy_SampleSessionsRespectsLimit verifies N caps the
// per-bucket array — three sessions touch llm_response but N=2 yields 2.
func TestEventsStats_GroupBy_SampleSessionsRespectsLimit(t *testing.T) {
	h := newTestHandler(t)
	// Add a third llm_response in a brand-new session so the bucket has
	// three distinct sessions. Most-recent timestamp goes to sess_c so
	// we can also confirm it lands at index 0 under N=2.
	if _, err := h.db.Exec(
		`INSERT INTO sessions VALUES ('sess_c','third',NULL,'gpt-4o','{}','user_3','group_z','2024-03-01T10:00:00Z','2024-03-01T10:30:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.Exec(
		`INSERT INTO session_events VALUES ('evt_c1','sess_c',1,'2024-03-01T10:00:00Z','llm_response',NULL,100,'{"v":1,"tokens_in":1,"tokens_out":1,"cost_input":0,"cost_output":0}','2024-03-01T10:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	w := do(t, h, "/events/stats?group_by=event_type&sample_sessions=2")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	llm := stats.ByEventType[0]
	if llm.EventType != "llm_response" {
		t.Fatalf("bucket[0] = %q, want llm_response", llm.EventType)
	}
	if len(llm.SampleSessionIDs) != 2 {
		t.Fatalf("len(SampleSessionIDs) = %d, want 2 (capped by N)", len(llm.SampleSessionIDs))
	}
	// Most-recent-first: sess_c (Mar), sess_b (Feb). sess_a (Jan) dropped.
	if llm.SampleSessionIDs[0] != "sess_c" || llm.SampleSessionIDs[1] != "sess_b" {
		t.Errorf("samples = %v, want [sess_c sess_b]", llm.SampleSessionIDs)
	}
}

func TestEventsStats_GroupBy_FilterStillApplies(t *testing.T) {
	// group_id=group_x → only sess_a. So buckets should reflect only
	// sess_a's events: llm_response=2 (no tool_call_result).
	w := do(t, newTestHandler(t), "/events/stats?group_by=event_type&group_id=group_x")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.ByEventType) != 1 {
		t.Fatalf("ByEventType len = %d, want 1 (only llm_response under group_x)", len(stats.ByEventType))
	}
	if stats.ByEventType[0].EventType != "llm_response" || stats.ByEventType[0].Count != 2 {
		t.Errorf("bucket = %+v, want {llm_response, 2}", stats.ByEventType[0])
	}
}

// TestEventsStats_GroupBy_EmptyWindowOmitsBuckets locks in the documented
// shape for the corner case "group_by requested over a filter that matches
// no events": top-level aggregates are zero, by_event_type is omitted
// (not present-but-empty). See the EventStats doc comment for the why —
// pointer-to-slice was rejected.
func TestEventsStats_GroupBy_EmptyWindowOmitsBuckets(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_by=event_type&entity_id=does_not_exist")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "by_event_type") {
		t.Errorf("empty-window response should omit by_event_type: %s", body)
	}
	var stats EventStats
	mustUnmarshal(t, []byte(body), &stats)
	if stats.SessionCount != 0 || stats.EventCount != 0 {
		t.Errorf("expected zero counts for empty window, got SessionCount=%d EventCount=%d",
			stats.SessionCount, stats.EventCount)
	}
}

func TestEventsStats_GroupBy_InvalidValue(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_by=session_id")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (group_by=session_id unsupported)", w.Code)
	}
}

func TestEventsStats_SampleSessions_RequiresGroupBy(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?sample_sessions=2")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (sample_sessions without group_by)", w.Code)
	}
}

func TestEventsStats_SampleSessions_OutOfRange(t *testing.T) {
	for _, ss := range []string{"0", "6", "100", "-1", "foo"} {
		w := do(t, newTestHandler(t), "/events/stats?group_by=event_type&sample_sessions="+ss)
		if w.Code != http.StatusBadRequest {
			t.Errorf("sample_sessions=%s: status = %d, want 400", ss, w.Code)
		}
	}
}

// TestEventsStats_GroupBy_EmptyValueTreatedAsAbsent locks in the handler's
// "empty == absent" contract: `?group_by=` (empty value, key present) is
// treated identically to omitting the param. Same for `?sample_sessions=`.
// This matches how filtersFromQuery handles entity_id="", and avoids
// surprising the consumer when they programmatically build URLs with
// optional defaults.
func TestEventsStats_GroupBy_EmptyValueTreatedAsAbsent(t *testing.T) {
	w := do(t, newTestHandler(t), "/events/stats?group_by=&sample_sessions=")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "by_event_type") {
		t.Errorf("empty group_by= should be treated as absent: %s", w.Body.String())
	}
}

// TestEventsStats_GroupBy_SampleSessionIDsTiebreaker exercises the
// (last_ts DESC, session_id ASC) ordering in fillSampleSessionIDs. With
// two sessions whose max-ts on the bucket's event type collide exactly,
// the session_id-ASC tiebreaker should produce a deterministic result —
// otherwise sample arrays drift across requests and dashboards flicker.
func TestEventsStats_GroupBy_SampleSessionIDsTiebreaker(t *testing.T) {
	h := newTestHandler(t)
	// Two new sessions whose llm_response events share the same ts.
	// session_id ASC means sess_alpha sorts before sess_beta.
	const sameTS = "2024-04-01T10:00:00Z"
	for _, sid := range []string{"sess_beta", "sess_alpha"} {
		if _, err := h.db.Exec(
			`INSERT INTO sessions VALUES (?,'tie',NULL,'gpt-4o','{}','user_tie','group_tie',?,?)`,
			sid, sameTS, sameTS); err != nil {
			t.Fatal(err)
		}
		evtID := "evt_" + sid
		if _, err := h.db.Exec(
			`INSERT INTO session_events VALUES (?,?,1,?,'llm_response',NULL,1,'{"v":1,"tokens_in":1,"tokens_out":1,"cost_input":0,"cost_output":0}',?)`,
			evtID, sid, sameTS, sameTS); err != nil {
			t.Fatal(err)
		}
	}
	// Scope to the tied group so we get a clean two-row result for samples.
	w := do(t, h, "/events/stats?group_by=event_type&sample_sessions=5&group_id=group_tie")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.ByEventType) != 1 || stats.ByEventType[0].EventType != "llm_response" {
		t.Fatalf("ByEventType = %+v, want one bucket (llm_response)", stats.ByEventType)
	}
	got := stats.ByEventType[0].SampleSessionIDs
	want := []string{"sess_alpha", "sess_beta"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("samples = %v, want %v (session_id ASC tiebreaker)", got, want)
	}
}

func TestPromptSnapshot_Get(t *testing.T) {
	w := do(t, newTestHandler(t), "/prompt-snapshots?sha=sha_sys_1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var p PromptSnapshot
	mustUnmarshal(t, w.Body.Bytes(), &p)
	if p.Content != "You are a helpful assistant." {
		t.Errorf("Content = %q", p.Content)
	}
}

func TestPromptSnapshot_MissingShaParam(t *testing.T) {
	w := do(t, newTestHandler(t), "/prompt-snapshots")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestPromptSnapshot_NotFound(t *testing.T) {
	w := do(t, newTestHandler(t), "/prompt-snapshots?sha=nope")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAuthMiddleware(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})

	h := authMiddleware("secret", inner)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 401 {
		t.Errorf("no token: status = %d, want 401", w.Code)
	}

	w = httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Errorf("wrong token: status = %d, want 401", w.Code)
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Errorf("correct token: status = %d, want 200", w.Code)
	}

	h2 := authMiddleware("", inner)
	w = httptest.NewRecorder()
	h2.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 {
		t.Errorf("no auth config: status = %d, want 200", w.Code)
	}
}

// --- /events/stats?bucket_by=… ---

// seedBucketEvents adds three sessions with deterministic timestamps in
// 2026-05 / 2026-06 / 2025-07 / 2027-02 to support the time-bucket tests.
// Layered on top of setupTestDB's existing 2024-01/02 fixtures — those
// stay outside the bucket-test windows so they don't perturb the
// expected counts. Returns nothing; the handler reads via h.db.
//
// Layout (all UTC):
//
//	sess_bk_alice  u_alice/acme    events on 2026-05-10 and 2026-05-12 (same session, two buckets)
//	sess_bk_bob    u_bob/acme      events on 2026-05-14 (llm + tool_call)
//	sess_bk_carol  u_carol/globex  event  on 2026-05-16
//	sess_bk_dave   u_dave/acme     event  on 2026-06-03 (next month, for month-bucket tests)
//	sess_bk_ed     u_ed/acme       event  on 2025-07-15 (prev year, for year-bucket tests)
func seedBucketEvents(t *testing.T, db *sql.DB) {
	t.Helper()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	llm := func(tokIn, tokOut int, costIn, costOut float64) string {
		return fmt.Sprintf(`{"v":1,"tokens_in":%d,"tokens_out":%d,"cost_input":%g,"cost_output":%g}`,
			tokIn, tokOut, costIn, costOut)
	}

	exec(`INSERT INTO sessions VALUES ('sess_bk_alice','',NULL,'gpt-4o','{}','u_alice','acme','2026-05-10T09:00:00Z','2026-05-12T11:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_bk_a1','sess_bk_alice',1,'2026-05-10T10:00:00Z','llm_response',NULL,100,?,'2026-05-10T10:00:00Z')`,
		llm(100, 50, 0.010, 0.020))
	exec(`INSERT INTO session_events VALUES ('evt_bk_a2','sess_bk_alice',2,'2026-05-12T10:00:00Z','llm_response',NULL,100,?,'2026-05-12T10:00:00Z')`,
		llm(200, 100, 0.020, 0.040))

	exec(`INSERT INTO sessions VALUES ('sess_bk_bob','',NULL,'gpt-4o','{}','u_bob','acme','2026-05-14T09:00:00Z','2026-05-14T12:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_bk_b1','sess_bk_bob',1,'2026-05-14T10:00:00Z','llm_response',NULL,200,?,'2026-05-14T10:00:00Z')`,
		llm(300, 150, 0.030, 0.060))
	exec(`INSERT INTO session_events VALUES ('evt_bk_b2','sess_bk_bob',2,'2026-05-14T11:00:00Z','tool_call_result','evt_bk_b1',50,'{"v":1,"result":"ok"}','2026-05-14T11:00:00Z')`)

	exec(`INSERT INTO sessions VALUES ('sess_bk_carol','',NULL,'gpt-4o','{}','u_carol','globex','2026-05-16T09:00:00Z','2026-05-16T10:30:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_bk_c1','sess_bk_carol',1,'2026-05-16T10:00:00Z','llm_response',NULL,100,?,'2026-05-16T10:00:00Z')`,
		llm(400, 200, 0.040, 0.080))

	exec(`INSERT INTO sessions VALUES ('sess_bk_dave','',NULL,'gpt-4o','{}','u_dave','acme','2026-06-03T09:00:00Z','2026-06-03T10:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_bk_d1','sess_bk_dave',1,'2026-06-03T10:00:00Z','llm_response',NULL,100,?,'2026-06-03T10:00:00Z')`,
		llm(500, 250, 0.050, 0.100))

	exec(`INSERT INTO sessions VALUES ('sess_bk_ed','',NULL,'gpt-4o','{}','u_ed','acme','2025-07-15T09:00:00Z','2025-07-15T10:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_bk_e1','sess_bk_ed',1,'2025-07-15T10:00:00Z','llm_response',NULL,100,?,'2025-07-15T10:00:00Z')`,
		llm(600, 300, 0.060, 0.120))
}

func newBucketHandler(t *testing.T) *Handler {
	h := newTestHandler(t)
	seedBucketEvents(t, h.db)
	return h
}

// bucketByKey indexes time_buckets by bucket key for compact assertions.
func bucketByKey(buckets []TimeBucket) map[string]TimeBucket {
	out := make(map[string]TimeBucket, len(buckets))
	for _, b := range buckets {
		out[b.Bucket] = b
	}
	return out
}

// — Issue #11 Test 1 —
// Verifies presence, ordering, and the populated keys for a 7-day window
// hitting the seed fixture; covers BucketsAndOrdering from the issue.
func TestEventsStats_BucketByDay_BucketsAndOrdering(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)

	wantKeys := []string{
		"2026-05-10", "2026-05-11", "2026-05-12", "2026-05-13",
		"2026-05-14", "2026-05-15", "2026-05-16",
	}
	if len(stats.TimeBuckets) != len(wantKeys) {
		t.Fatalf("len(TimeBuckets) = %d, want %d; buckets = %+v",
			len(stats.TimeBuckets), len(wantKeys), stats.TimeBuckets)
	}
	for i, want := range wantKeys {
		if stats.TimeBuckets[i].Bucket != want {
			t.Errorf("TimeBuckets[%d].Bucket = %q, want %q", i, stats.TimeBuckets[i].Bucket, want)
		}
	}
}

// — Issue #11 Test 2 — empty days filled in with zero counters.
func TestEventsStats_BucketByDay_EmptyDaysFilled(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)

	for _, emptyDay := range []string{"2026-05-11", "2026-05-13", "2026-05-15"} {
		b, ok := byKey[emptyDay]
		if !ok {
			t.Fatalf("expected empty bucket for %s; got buckets = %+v", emptyDay, stats.TimeBuckets)
		}
		if b.SessionCount != 0 || b.EventCount != 0 || b.LLMCallCount != 0 ||
			b.ToolCallCount != 0 || b.TokensInTotal != 0 || b.TokensOutTotal != 0 ||
			b.CostInputTotal != 0 || b.CostOutputTotal != 0 {
			t.Errorf("empty bucket %s has non-zero counter: %+v", emptyDay, b)
		}
	}
}

// — Issue #11 Test 3 — narrow window only returns its subset of buckets.
func TestEventsStats_BucketByDay_RespectsSinceUntil(t *testing.T) {
	h := newBucketHandler(t)
	// Narrow to 2026-05-12 and 2026-05-13 only.
	w := do(t, h, "/events/stats?bucket_by=day&since=2026-05-12T00:00:00Z&until=2026-05-14T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 2 {
		t.Fatalf("len(TimeBuckets) = %d, want 2; got %+v", len(stats.TimeBuckets), stats.TimeBuckets)
	}
	if stats.TimeBuckets[0].Bucket != "2026-05-12" || stats.TimeBuckets[1].Bucket != "2026-05-13" {
		t.Errorf("buckets = %+v, want [2026-05-12, 2026-05-13]", stats.TimeBuckets)
	}
	// Bucket 2026-05-12 has evt_bk_a2 (sess_bk_alice's second event).
	if stats.TimeBuckets[0].EventCount != 1 || stats.TimeBuckets[0].TokensInTotal != 200 {
		t.Errorf("bucket 2026-05-12 = %+v, want EventCount=1, TokensInTotal=200", stats.TimeBuckets[0])
	}
}

// — Issue #11 Test 4 — group_id scoping applied to buckets.
func TestEventsStats_BucketByDay_RespectsGroupID(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&group_id=globex&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)
	// Only sess_bk_carol matches; its event is on 2026-05-16.
	if b := byKey["2026-05-16"]; b.EventCount != 1 || b.TokensInTotal != 400 {
		t.Errorf("globex on 2026-05-16 = %+v, want EventCount=1, TokensInTotal=400", b)
	}
	for _, key := range []string{"2026-05-10", "2026-05-12", "2026-05-14"} {
		if b := byKey[key]; b.EventCount != 0 {
			t.Errorf("globex on %s = %+v, want EventCount=0 (different group)", key, b)
		}
	}
	if stats.EventCount != 1 || stats.SessionCount != 1 {
		t.Errorf("top-level (globex only) = %+v, want EventCount=1 SessionCount=1", stats)
	}
}

// — Issue #11 Test 5 — entity_id (singular) scoping applied to buckets.
func TestEventsStats_BucketByDay_RespectsEntityID(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&entity_id=u_alice&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)
	if b := byKey["2026-05-10"]; b.EventCount != 1 || b.TokensInTotal != 100 {
		t.Errorf("alice on 2026-05-10 = %+v, want EventCount=1 TokensInTotal=100", b)
	}
	if b := byKey["2026-05-12"]; b.EventCount != 1 || b.TokensInTotal != 200 {
		t.Errorf("alice on 2026-05-12 = %+v, want EventCount=1 TokensInTotal=200", b)
	}
	// Bob's events on 14, Carol's on 16 must be excluded.
	for _, key := range []string{"2026-05-14", "2026-05-16"} {
		if b := byKey[key]; b.EventCount != 0 {
			t.Errorf("alice on %s = %+v, want EventCount=0 (different user)", key, b)
		}
	}
}

// — Issue #11 Test 6 — include_entity_ids (multi-actor) scoping.
func TestEventsStats_BucketByDay_RespectsIncludeEntityIDs(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&include_entity_ids=u_alice,u_bob&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	// Alice (10+12) + Bob (14) — carol's 16 excluded.
	// Top-level totals: 4 events (a1,a2,b1,b2), 3 LLM calls, 1 tool_call,
	// tokens_in 100+200+300 = 600.
	if stats.EventCount != 4 || stats.LLMCallCount != 3 || stats.ToolCallCount != 1 {
		t.Errorf("totals = %+v, want EventCount=4 LLMCallCount=3 ToolCallCount=1", stats)
	}
	if stats.TokensInTotal != 600 {
		t.Errorf("TokensInTotal = %d, want 600", stats.TokensInTotal)
	}
	byKey := bucketByKey(stats.TimeBuckets)
	if b := byKey["2026-05-16"]; b.EventCount != 0 {
		t.Errorf("carol's day 2026-05-16 = %+v, want EventCount=0 (excluded by include list)", b)
	}
}

// — Issue #11 Test 7 — exclude_entity_ids scoping (subtract internal staff).
func TestEventsStats_BucketByDay_RespectsExcludeEntityIDs(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&exclude_entity_ids=u_alice&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)
	// Alice's days (10, 12) must be empty post-exclude.
	for _, key := range []string{"2026-05-10", "2026-05-12"} {
		if b := byKey[key]; b.EventCount != 0 {
			t.Errorf("excluded alice on %s = %+v, want EventCount=0", key, b)
		}
	}
	// Bob (14) and Carol (16) remain.
	if b := byKey["2026-05-14"]; b.EventCount != 2 {
		t.Errorf("bob on 2026-05-14 = %+v, want EventCount=2", b)
	}
	if b := byKey["2026-05-16"]; b.EventCount != 1 {
		t.Errorf("carol on 2026-05-16 = %+v, want EventCount=1", b)
	}
}

// — Issue #11 Test 8 — bucket_by requires since AND until.
func TestEventsStats_BucketByDay_RejectedWithoutSinceUntil(t *testing.T) {
	h := newBucketHandler(t)
	for _, target := range []string{
		"/events/stats?bucket_by=day",
		"/events/stats?bucket_by=day&since=2026-05-10T00:00:00Z",
		"/events/stats?bucket_by=day&until=2026-05-17T00:00:00Z",
	} {
		w := do(t, h, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body = %s", target, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "since") || !strings.Contains(w.Body.String(), "until") {
			t.Errorf("%s: body should name since/until, got %s", target, w.Body.String())
		}
	}
}

// — Issue #11 Test 9 — bucket count above per-granularity cap → 400.
func TestEventsStats_BucketByDay_RejectedAboveMaxBuckets(t *testing.T) {
	h := newBucketHandler(t)
	// 1201 days > maxBucketCountDay (1200).
	w := do(t, h, "/events/stats?bucket_by=day&since=2023-01-01T00:00:00Z&until=2026-04-17T00:00:00Z")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "buckets") {
		t.Errorf("body should mention bucket cap, got %s", w.Body.String())
	}
}

// — Issue #11 Test 10 — default response shape unchanged when bucket_by absent.
func TestEventsStats_BucketByDay_DefaultShapeUnchanged(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats")
	if strings.Contains(w.Body.String(), "time_buckets") {
		t.Errorf("default response leaked time_buckets key: %s", w.Body.String())
	}
}

// — Issue #11 Test 11 (renamed Precedence → Intersection) —
// entity_id + include_entity_ids AND together (intersection), not
// precedence. The PR #9 contract is preserved under bucket_by.
func TestEventsStats_EntityIDVsIncludeEntityIDs_Intersection(t *testing.T) {
	h := newBucketHandler(t)
	// Singular pins to alice; the include list adds bob — intersection is
	// just alice. If precedence semantics applied, both alice + bob would
	// appear.
	w := do(t, h, "/events/stats?bucket_by=day&entity_id=u_alice&include_entity_ids=u_alice,u_bob&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	// Alice alone: 2 events on 10 + 12. Bob's 2 events on 14 must not appear.
	if stats.EventCount != 2 {
		t.Errorf("EventCount = %d, want 2 (intersection = alice only)", stats.EventCount)
	}
	byKey := bucketByKey(stats.TimeBuckets)
	if b := byKey["2026-05-14"]; b.EventCount != 0 {
		t.Errorf("bob's day 2026-05-14 = %+v, want EventCount=0 (bob not in intersection)", b)
	}
}

// --- Edge cases beyond the issue list ---

// bucket_by + group_by=event_type → 400 (cross-tab deferred in v1).
func TestEventsStats_BucketByDay_RejectedWithGroupBy(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&group_by=event_type&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "group_by") {
		t.Errorf("body should mention group_by conflict, got %s", w.Body.String())
	}
}

// Unknown bucket_by value → 400 with allow-list in error.
func TestEventsStats_BucketByDay_RejectedInvalidValue(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=hour&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	for _, allowed := range []string{"day", "week", "month", "year"} {
		if !strings.Contains(w.Body.String(), allowed) {
			t.Errorf("body should list %q as allowed, got %s", allowed, w.Body.String())
		}
	}
}

// since == until → 400 (degenerate empty window).
func TestEventsStats_BucketByDay_RejectedSinceEqualsUntil(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-10T00:00:00Z")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

// Σ(time_buckets[].counter) == top-level counter for everything except
// session_count, which is distinct top-level vs. per-bucket distinct.
func TestEventsStats_BucketByDay_SumConsistency(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)

	var (
		sumEvent          int64
		sumLLM, sumTool   int64
		sumTokIn, sumTout int64
		sumCostIn, sumCo  float64
		sumSession        int
	)
	for _, b := range stats.TimeBuckets {
		sumEvent += b.EventCount
		sumLLM += b.LLMCallCount
		sumTool += b.ToolCallCount
		sumTokIn += b.TokensInTotal
		sumTout += b.TokensOutTotal
		sumCostIn += b.CostInputTotal
		sumCo += b.CostOutputTotal
		sumSession += b.SessionCount
	}
	if sumEvent != stats.EventCount {
		t.Errorf("sum EventCount = %d, top-level = %d", sumEvent, stats.EventCount)
	}
	if sumLLM != stats.LLMCallCount {
		t.Errorf("sum LLMCallCount = %d, top-level = %d", sumLLM, stats.LLMCallCount)
	}
	if sumTool != stats.ToolCallCount {
		t.Errorf("sum ToolCallCount = %d, top-level = %d", sumTool, stats.ToolCallCount)
	}
	if sumTokIn != stats.TokensInTotal {
		t.Errorf("sum TokensInTotal = %d, top-level = %d", sumTokIn, stats.TokensInTotal)
	}
	if sumTout != stats.TokensOutTotal {
		t.Errorf("sum TokensOutTotal = %d, top-level = %d", sumTout, stats.TokensOutTotal)
	}
	if sumCostIn != stats.CostInputTotal {
		t.Errorf("sum CostInputTotal = %g, top-level = %g", sumCostIn, stats.CostInputTotal)
	}
	if sumCo != stats.CostOutputTotal {
		t.Errorf("sum CostOutputTotal = %g, top-level = %g", sumCo, stats.CostOutputTotal)
	}
	// session_count: alice spans 10 and 12 → counted in 2 buckets, but
	// top-level distinct = 1 for her. With bob (14) and carol (16) the
	// sum should be 4, top-level 3. Documented invariant: ≥, not ==.
	if sumSession < stats.SessionCount {
		t.Errorf("Σ buckets session_count = %d, top-level = %d (must be ≥)", sumSession, stats.SessionCount)
	}
	if sumSession != 4 || stats.SessionCount != 3 {
		t.Errorf("session counts = (Σ=%d, top=%d), want (4, 3) for the seed fixture", sumSession, stats.SessionCount)
	}
}

// A session active on two distinct days contributes session_count=1 to
// each of its day-buckets — the doc-warned multi-counting behaviour.
func TestEventsStats_BucketByDay_SessionSpansMultipleBucketsCounted(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=day&entity_id=u_alice&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)
	if b := byKey["2026-05-10"]; b.SessionCount != 1 {
		t.Errorf("alice 2026-05-10 SessionCount = %d, want 1", b.SessionCount)
	}
	if b := byKey["2026-05-12"]; b.SessionCount != 1 {
		t.Errorf("alice 2026-05-12 SessionCount = %d, want 1", b.SessionCount)
	}
	// Top-level distinct session for alice = 1 (sess_bk_alice only).
	if stats.SessionCount != 1 {
		t.Errorf("alice top-level SessionCount = %d, want 1", stats.SessionCount)
	}
}

// Empty window (no events at all) still returns the full bucket scaffold
// with all-zero counters — keeps the chart x-axis continuous.
func TestEventsStats_BucketByDay_EmptyWindowAllZeros(t *testing.T) {
	h := newBucketHandler(t)
	// 2030 has no fixture events at all.
	w := do(t, h, "/events/stats?bucket_by=day&since=2030-01-01T00:00:00Z&until=2030-01-04T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 3 {
		t.Fatalf("len(TimeBuckets) = %d, want 3 (full scaffold even with zero events); got %+v",
			len(stats.TimeBuckets), stats.TimeBuckets)
	}
	for _, b := range stats.TimeBuckets {
		if b.EventCount != 0 || b.SessionCount != 0 {
			t.Errorf("bucket %s should be all zeros, got %+v", b.Bucket, b)
		}
	}
	if stats.EventCount != 0 || stats.SessionCount != 0 {
		t.Errorf("top-level should be all zeros, got %+v", stats)
	}
}

// --- Granularity-specific tests ---

// Week buckets are Monday-anchored (ISO 8601), regardless of the day
// since/until falls on.
func TestEventsStats_BucketByWeek_MondayStart(t *testing.T) {
	h := newBucketHandler(t)
	// 2026-05-10 is a Sunday. The week containing 10/12/14/16 starts on
	// 2026-05-04 (Monday) for the 4–10 window; the week 11–17 starts on
	// 2026-05-11. Querying since/until that spans both weeks:
	w := do(t, h, "/events/stats?bucket_by=week&since=2026-05-04T00:00:00Z&until=2026-05-18T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	// Expect two Monday-anchored buckets.
	if len(stats.TimeBuckets) != 2 {
		t.Fatalf("len(TimeBuckets) = %d, want 2; got %+v", len(stats.TimeBuckets), stats.TimeBuckets)
	}
	wantKeys := []string{"2026-05-04", "2026-05-11"}
	for i, key := range wantKeys {
		if stats.TimeBuckets[i].Bucket != key {
			t.Errorf("TimeBuckets[%d].Bucket = %q, want %q", i, stats.TimeBuckets[i].Bucket, key)
		}
	}
	// Week 2026-05-04 contains alice on Sun 2026-05-10 only.
	if stats.TimeBuckets[0].EventCount != 1 {
		t.Errorf("week 2026-05-04 EventCount = %d, want 1 (alice's Sunday event)", stats.TimeBuckets[0].EventCount)
	}
	// Week 2026-05-11 contains alice (12), bob (14×2), carol (16).
	if stats.TimeBuckets[1].EventCount != 4 {
		t.Errorf("week 2026-05-11 EventCount = %d, want 4", stats.TimeBuckets[1].EventCount)
	}
}

// Month bucket keys are the 1st of the month.
func TestEventsStats_BucketByMonth_FirstOfMonth(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?bucket_by=month&since=2026-05-01T00:00:00Z&until=2026-07-01T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 2 {
		t.Fatalf("len = %d, want 2; got %+v", len(stats.TimeBuckets), stats.TimeBuckets)
	}
	if stats.TimeBuckets[0].Bucket != "2026-05-01" || stats.TimeBuckets[1].Bucket != "2026-06-01" {
		t.Errorf("buckets = [%q, %q], want [2026-05-01, 2026-06-01]",
			stats.TimeBuckets[0].Bucket, stats.TimeBuckets[1].Bucket)
	}
	// May has alice (2) + bob (2) + carol (1) = 5 events.
	if stats.TimeBuckets[0].EventCount != 5 {
		t.Errorf("May EventCount = %d, want 5", stats.TimeBuckets[0].EventCount)
	}
	// June has dave (1).
	if stats.TimeBuckets[1].EventCount != 1 {
		t.Errorf("June EventCount = %d, want 1", stats.TimeBuckets[1].EventCount)
	}
}

// Year bucket keys are Jan 1, partial edge buckets are OK.
func TestEventsStats_BucketByYear_JanuaryFirst(t *testing.T) {
	h := newBucketHandler(t)
	// 2025 has Ed's event; 2026 has alice/bob/carol/dave.
	w := do(t, h, "/events/stats?bucket_by=year&since=2025-01-01T00:00:00Z&until=2027-01-01T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 2 {
		t.Fatalf("len = %d, want 2; got %+v", len(stats.TimeBuckets), stats.TimeBuckets)
	}
	if stats.TimeBuckets[0].Bucket != "2025-01-01" || stats.TimeBuckets[1].Bucket != "2026-01-01" {
		t.Errorf("buckets = [%q, %q], want [2025-01-01, 2026-01-01]",
			stats.TimeBuckets[0].Bucket, stats.TimeBuckets[1].Bucket)
	}
	if stats.TimeBuckets[0].EventCount != 1 {
		t.Errorf("2025 EventCount = %d, want 1 (ed only)", stats.TimeBuckets[0].EventCount)
	}
	// 2026: alice(2) + bob(2) + carol(1) + dave(1) = 6.
	if stats.TimeBuckets[1].EventCount != 6 {
		t.Errorf("2026 EventCount = %d, want 6", stats.TimeBuckets[1].EventCount)
	}
}

// Edge buckets are allowed to be partial: bucket-key reflects the
// natural period, but counters only include events inside [since, until).
func TestEventsStats_BucketByMonth_EdgeBucketsPartial(t *testing.T) {
	h := newBucketHandler(t)
	// Window 2026-05-13 → 2026-06-10. May bucket (key 2026-05-01) only
	// contains events from 13 onwards; June bucket (key 2026-06-01) only
	// up to 10. May data on/after 13: bob(2)+carol(1)=3 (alice 10+12 excluded).
	// June data through 10: dave on 06-03 = 1.
	w := do(t, h, "/events/stats?bucket_by=month&since=2026-05-13T00:00:00Z&until=2026-06-10T00:00:00Z")
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 2 {
		t.Fatalf("len = %d, want 2; got %+v", len(stats.TimeBuckets), stats.TimeBuckets)
	}
	if stats.TimeBuckets[0].Bucket != "2026-05-01" {
		t.Errorf("first edge bucket = %q, want 2026-05-01 (natural month start, partial)", stats.TimeBuckets[0].Bucket)
	}
	if stats.TimeBuckets[0].EventCount != 3 {
		t.Errorf("partial May EventCount = %d, want 3 (only 13 onwards)", stats.TimeBuckets[0].EventCount)
	}
	if stats.TimeBuckets[1].EventCount != 1 {
		t.Errorf("partial June EventCount = %d, want 1", stats.TimeBuckets[1].EventCount)
	}
	// Sum invariant survives partial edges.
	if stats.EventCount != 4 {
		t.Errorf("top-level EventCount = %d, want 4 (= sum of partial buckets)", stats.EventCount)
	}
}

// --- bucketCountExceeds unit coverage (off-by-one boundary) ---
// At exactly the cap, the request must succeed; one bucket over, it 400s.
func TestEventsStats_BucketByDay_BoundaryAtCap(t *testing.T) {
	h := newBucketHandler(t)
	// Exactly 1200 daily buckets — at the cap, not over. since...until
	// spans 1200 days; truncation in fillEmptyBuckets is identical to the
	// validator, so this is the precise off-by-one we want to lock in.
	since := "2024-01-01T00:00:00Z"
	until := "2027-04-15T00:00:00Z" // 2024 (366) + 2025 (365) + 2026 (365) + 104 = 1200
	w := do(t, h, fmt.Sprintf("/events/stats?bucket_by=day&since=%s&until=%s", since, until))
	if w.Code != http.StatusOK {
		t.Fatalf("at-cap (1200 days) status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if len(stats.TimeBuckets) != 1200 {
		t.Errorf("at-cap len = %d, want 1200", len(stats.TimeBuckets))
	}

	// One day over: 1201 → 400.
	overUntil := "2027-04-16T00:00:00Z"
	w = do(t, h, fmt.Sprintf("/events/stats?bucket_by=day&since=%s&until=%s", since, overUntil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("over-cap (1201 days) status = %d, want 400", w.Code)
	}
}

// --- Time-axis contract: since/until filter on se.ts uniformly ---
//
// Locking tests for the cross-endpoint decision (PR #12 follow-up): the
// since/until window selects "events that occurred in the window", not
// "sessions created in the window". A session with created_at outside
// the window but events inside must contribute; a session with
// created_at inside but events outside must NOT.
//
// These tests are explicit because the surrounding tests use a fixture
// where se.ts == s.created_at and therefore can't distinguish the two
// axes. They use their own seed to force the axes apart.

// seedSpanningSessions adds two sessions designed to be distinguishable
// only by which axis (s.created_at vs se.ts) the time filter uses.
//
//	sess_span_inside  created on 2026-04-30 (BEFORE window), event on 2026-05-15 (INSIDE window)
//	sess_span_outside created on 2026-05-05 (INSIDE window), event on 2026-04-10 (BEFORE window)
//
// Filtering window: [2026-05-01, 2026-06-01).
// Under se.ts (current contract): sess_span_inside matches, sess_span_outside doesn't.
// Under s.created_at (old contract): sess_span_outside matches, sess_span_inside doesn't.
func seedSpanningSessions(t *testing.T, db *sql.DB) {
	t.Helper()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	llm := func(tokIn, tokOut int, costIn, costOut float64) string {
		return fmt.Sprintf(`{"v":1,"tokens_in":%d,"tokens_out":%d,"cost_input":%g,"cost_output":%g}`,
			tokIn, tokOut, costIn, costOut)
	}

	// Container created APRIL, event in MAY — under se.ts contract, IN window.
	exec(`INSERT INTO sessions VALUES ('sess_span_inside','',NULL,'gpt-4o','{}','u_ts_inside','tsax','2026-04-30T23:00:00Z','2026-05-15T11:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_ts_inside','sess_span_inside',1,'2026-05-15T10:00:00Z','llm_response',NULL,100,?,'2026-05-15T10:00:00Z')`,
		llm(700, 350, 0.070, 0.140))

	// Container created MAY, event in APRIL — under se.ts contract, OUT of window.
	exec(`INSERT INTO sessions VALUES ('sess_span_outside','',NULL,'gpt-4o','{}','u_ts_outside','tsax','2026-05-05T09:00:00Z','2026-05-05T10:00:00Z')`)
	exec(`INSERT INTO session_events VALUES ('evt_ts_outside','sess_span_outside',1,'2026-04-10T10:00:00Z','llm_response',NULL,100,?,'2026-04-10T10:00:00Z')`,
		llm(800, 400, 0.080, 0.160))
}

func newSpanningHandler(t *testing.T) *Handler {
	h := newTestHandler(t)
	seedSpanningSessions(t, h.db)
	return h
}

// /sessions list applies since/until on se.ts: the container-in-April
// session with event-in-May appears; the container-in-May with
// event-in-April does NOT. Locks the cross-endpoint contract.
func TestListSessions_TimeAxisIsEventTimestamp(t *testing.T) {
	h := newSpanningHandler(t)
	w := do(t, h, "/sessions?group_id=tsax&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp SessionListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	gotIDs := map[string]bool{}
	for _, s := range resp.Items {
		gotIDs[s.ID] = true
	}
	if !gotIDs["sess_span_inside"] {
		t.Errorf("sess_span_inside (event in window) missing from /sessions list: got %v", gotIDs)
	}
	if gotIDs["sess_span_outside"] {
		t.Errorf("sess_span_outside (event outside window) leaked into /sessions list: got %v", gotIDs)
	}
}

// /events list applies since/until on se.ts: only the May event appears.
func TestListEvents_TimeAxisIsEventTimestamp(t *testing.T) {
	h := newSpanningHandler(t)
	w := do(t, h, "/events?group_id=tsax&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp EventListResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	gotIDs := map[string]bool{}
	for _, e := range resp.Items {
		gotIDs[e.ID] = true
	}
	if !gotIDs["evt_ts_inside"] {
		t.Errorf("evt_ts_inside (in window) missing from /events list: got %v", gotIDs)
	}
	if gotIDs["evt_ts_outside"] {
		t.Errorf("evt_ts_outside (outside window) leaked into /events list: got %v", gotIDs)
	}
}

// /events/stats (no bucket_by) totals reflect se.ts axis — locks the
// behavior change introduced with the unified time-axis contract.
func TestEventsStats_TimeAxisIsEventTimestamp(t *testing.T) {
	h := newSpanningHandler(t)
	w := do(t, h, "/events/stats?group_id=tsax&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)

	// Only sess_span_inside's event contributes: 1 session, 1 event,
	// 1 llm_response, tokens_in=700, cost_input=0.070.
	if stats.SessionCount != 1 {
		t.Errorf("SessionCount = %d, want 1 (sess_span_inside only)", stats.SessionCount)
	}
	if stats.EventCount != 1 {
		t.Errorf("EventCount = %d, want 1", stats.EventCount)
	}
	if stats.LLMCallCount != 1 {
		t.Errorf("LLMCallCount = %d, want 1", stats.LLMCallCount)
	}
	if stats.TokensInTotal != 700 {
		t.Errorf("TokensInTotal = %d, want 700 (sess_span_outside's 800 must NOT contribute)", stats.TokensInTotal)
	}
	if stats.CostInputTotal != 0.070 {
		t.Errorf("CostInputTotal = %g, want 0.070", stats.CostInputTotal)
	}
}

// /events/stats?bucket_by= same axis (was always se.ts; locked now in
// case anyone "fixes" the time-axis back to s.created_at later).
func TestEventsStats_BucketBy_TimeAxisIsEventTimestamp(t *testing.T) {
	h := newSpanningHandler(t)
	w := do(t, h, "/events/stats?group_id=tsax&bucket_by=day&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)

	byKey := bucketByKey(stats.TimeBuckets)
	// Event-in-window day must have data.
	if b := byKey["2026-05-15"]; b.EventCount != 1 || b.TokensInTotal != 700 {
		t.Errorf("2026-05-15 bucket = %+v, want EventCount=1 TokensInTotal=700", b)
	}
	// The container-in-window session has no event in the window — no
	// stray bucket key from its created_at date.
	if b, present := byKey["2026-05-05"]; present && (b.EventCount > 0 || b.SessionCount > 0) {
		t.Errorf("2026-05-05 bucket leaked nonzero data from container-in-window: %+v", b)
	}
}

// All four endpoints answer consistently for the same since/until window.
// If anyone ever splits the axis back across endpoints, this catches it.
func TestTimeAxis_ConsistentAcrossEndpoints(t *testing.T) {
	h := newSpanningHandler(t)
	common := "group_id=tsax&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z"

	// /sessions: exactly 1 session (sess_span_inside).
	var listResp SessionListResponse
	mustUnmarshal(t, do(t, h, "/sessions?"+common).Body.Bytes(), &listResp)
	if len(listResp.Items) != 1 || listResp.Items[0].ID != "sess_span_inside" {
		t.Errorf("/sessions returned %d items (%v), want exactly [sess_span_inside]",
			len(listResp.Items), itemIDs(listResp.Items))
	}

	// /events: exactly 1 event (evt_ts_inside).
	var evResp EventListResponse
	mustUnmarshal(t, do(t, h, "/events?"+common).Body.Bytes(), &evResp)
	if len(evResp.Items) != 1 || evResp.Items[0].ID != "evt_ts_inside" {
		t.Errorf("/events returned %d items, want exactly [evt_ts_inside]", len(evResp.Items))
	}

	// /events/stats (default): SessionCount=1, EventCount=1.
	var statsDefault EventStats
	mustUnmarshal(t, do(t, h, "/events/stats?"+common).Body.Bytes(), &statsDefault)
	if statsDefault.SessionCount != 1 || statsDefault.EventCount != 1 {
		t.Errorf("/events/stats default = (sess=%d, ev=%d), want (1, 1)",
			statsDefault.SessionCount, statsDefault.EventCount)
	}

	// /events/stats?bucket_by=day: top-level matches the default path.
	var statsBucket EventStats
	mustUnmarshal(t, do(t, h, "/events/stats?bucket_by=day&"+common).Body.Bytes(), &statsBucket)
	if statsBucket.SessionCount != statsDefault.SessionCount {
		t.Errorf("bucket-path SessionCount = %d, default SessionCount = %d (must match)",
			statsBucket.SessionCount, statsDefault.SessionCount)
	}
	if statsBucket.EventCount != statsDefault.EventCount {
		t.Errorf("bucket-path EventCount = %d, default EventCount = %d (must match)",
			statsBucket.EventCount, statsDefault.EventCount)
	}
}

func itemIDs(items []SessionListItem) []string {
	out := make([]string, len(items))
	for i, s := range items {
		out[i] = s.ID
	}
	return out
}

// --- Validation strictness (cleanup follow-up to PR #12) ---

// include_payload accepts only literal "true"/"false"; anything else 400.
// Locks the contract that silent fallback (pre-cleanup behavior) is gone.
func TestListEvents_IncludePayloadValidation(t *testing.T) {
	h := newTestHandler(t)

	for _, target := range []string{
		"/events?include_payload=true",
		"/events?include_payload=false",
		"/events", // empty → default true
	} {
		if w := do(t, h, target); w.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200; body = %s", target, w.Code, w.Body.String())
		}
	}

	for _, target := range []string{
		"/events?include_payload=banana",
		"/events?include_payload=1",
		"/events?include_payload=TRUE",
		"/events?include_payload=yes",
	} {
		w := do(t, h, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body = %s", target, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "include_payload") {
			t.Errorf("%s: error body should name include_payload, got %s", target, w.Body.String())
		}
	}
}

// limit: any value outside (1..maxLimit] → 400. Matches the strictness
// of every other cap in the API (entity ID lists, sample_sessions,
// bucket counts). Consumer wanting more rows uses cursor pagination.
func TestLimitValidation_StrictOnAllBounds(t *testing.T) {
	h := newBucketHandler(t)

	for _, target := range []string{
		// Garbage / non-positive
		"/sessions?limit=banana",
		"/sessions?limit=0",
		"/sessions?limit=-5",
		"/events?limit=banana",
		"/events?limit=0",
		// Over-cap
		"/sessions?limit=999999",
		"/sessions?limit=201",
		"/events?limit=201",
	} {
		w := do(t, h, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body = %s", target, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "limit") {
			t.Errorf("%s: error body should name limit, got %s", target, w.Body.String())
		}
	}

	// At cap (exactly maxLimit) — still 200 OK, boundary case.
	w := do(t, h, "/sessions?limit=200")
	if w.Code != http.StatusOK {
		t.Fatalf("/sessions?limit=200 (at-cap): status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
}

// --- event_type filter on /events/stats (cleanup follow-up to PR #12) ---

// event_type filter narrows totals to a single event type. Sub-counters
// for the other types zero out; session_count drops to "sessions with
// at least one event of this type".
func TestEventsStats_EventTypeFilter_NarrowsTotals(t *testing.T) {
	h := newBucketHandler(t)

	// All LLM events in May 2026: alice(2) + bob(1) + carol(1) = 4 events.
	w := do(t, h, "/events/stats?event_type=llm_response&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.EventCount != 4 {
		t.Errorf("EventCount = %d, want 4 (llm_response only)", stats.EventCount)
	}
	if stats.LLMCallCount != 4 {
		t.Errorf("LLMCallCount = %d, want 4", stats.LLMCallCount)
	}
	if stats.ToolCallCount != 0 {
		t.Errorf("ToolCallCount = %d, want 0 (filtered out)", stats.ToolCallCount)
	}
	if stats.SessionCount != 3 {
		t.Errorf("SessionCount = %d, want 3 (alice, bob, carol all have at least one llm_response)", stats.SessionCount)
	}

	// Only tool_call_result events: just bob has one in May.
	w = do(t, h, "/events/stats?event_type=tool_call_result&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	if stats.EventCount != 1 || stats.ToolCallCount != 1 || stats.LLMCallCount != 0 {
		t.Errorf("tool_call_result totals = %+v, want EventCount=1 ToolCallCount=1 LLMCallCount=0", stats)
	}
	if stats.TokensInTotal != 0 || stats.CostInputTotal != 0 {
		t.Errorf("tool_call_result tokens/cost should be 0 (no llm payload), got tokens=%d cost=%g",
			stats.TokensInTotal, stats.CostInputTotal)
	}
}

// event_type filter applies to time_buckets too: window+filter narrows
// per-bucket counters to only that event type.
func TestEventsStats_EventTypeFilter_AppliesToBuckets(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?event_type=tool_call_result&bucket_by=day&since=2026-05-10T00:00:00Z&until=2026-05-17T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var stats EventStats
	mustUnmarshal(t, w.Body.Bytes(), &stats)
	byKey := bucketByKey(stats.TimeBuckets)

	// Bob's tool_call_result on 2026-05-14 is the only event.
	if b := byKey["2026-05-14"]; b.EventCount != 1 || b.ToolCallCount != 1 {
		t.Errorf("2026-05-14 = %+v, want EventCount=1 ToolCallCount=1", b)
	}
	// All other days zero (alice/carol's llm_response events are filtered out).
	for _, key := range []string{"2026-05-10", "2026-05-12", "2026-05-16"} {
		if b := byKey[key]; b.EventCount != 0 {
			t.Errorf("%s = %+v, want EventCount=0 (no tool_call_result)", key, b)
		}
	}
}

// event_type + group_by=event_type → 400 (redundant: grouping collapses
// to one bucket). Locks the validation that prevents the silly call.
func TestEventsStats_EventTypeFilter_RejectedWithGroupBy(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?event_type=llm_response&group_by=event_type")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "event_type") || !strings.Contains(w.Body.String(), "group_by") {
		t.Errorf("body should mention event_type + group_by conflict, got %s", w.Body.String())
	}
}

// event_type + bucket_by is allowed (useful: time-series for one type).
// Covered by the AppliesToBuckets test above, but explicit smoke here.
func TestEventsStats_EventTypeFilter_BucketByCombineAllowed(t *testing.T) {
	h := newBucketHandler(t)
	w := do(t, h, "/events/stats?event_type=llm_response&bucket_by=month&since=2026-05-01T00:00:00Z&until=2026-07-01T00:00:00Z")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

// Empty filter (?event_type=) is treated as absent — same shape as no param.
func TestEventsStats_EventTypeFilter_EmptyIsAbsent(t *testing.T) {
	h := newBucketHandler(t)
	w1 := do(t, h, "/events/stats?since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	w2 := do(t, h, "/events/stats?event_type=&since=2026-05-01T00:00:00Z&until=2026-06-01T00:00:00Z")
	if w1.Body.String() != w2.Body.String() {
		t.Errorf("?event_type= should equal no param:\n  no param: %s\n  empty:    %s",
			w1.Body.String(), w2.Body.String())
	}
}

// --- DB-error path coverage (cleanup follow-up) ---
//
// Each handler that returns 500 on internal errors must (a) hide the
// raw DB-error text from the client and (b) still return a JSON-shaped
// error body. The handlers also log server-side via writeServerErr,
// but that's not asserted here — log capture isn't worth the test
// scaffolding for one assertion.

// /health returns 503 when the DB ping fails. Closes the DB before
// invoking the handler to force the ping failure deterministically.
func TestHealth_DBUnreachable(t *testing.T) {
	h := newTestHandler(t)
	_ = h.db.Close()
	w := do(t, h, "/health")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "db unreachable") {
		t.Errorf("body should mention db unreachable, got %s", w.Body.String())
	}
}

// /sessions/{id} returns 500 (not 404) on a non-ErrNoRows DB error,
// and the response body must NOT leak the raw DB error text.
func TestGetSession_DBError(t *testing.T) {
	h := newTestHandler(t)
	_ = h.db.Close() // force any subsequent query to error
	w := do(t, h, "/sessions/sess_a")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "internal server error") {
		t.Errorf("body should contain generic 'internal server error', got %s", body)
	}
	// Spot-check that we don't leak DB internals to the client.
	for _, leak := range []string{"sql:", "sqlite", "database is closed"} {
		if strings.Contains(body, leak) {
			t.Errorf("body leaks internal error detail %q: %s", leak, body)
		}
	}
}

// /prompt-snapshots returns 500 with generic message on DB error.
func TestPromptSnapshot_DBError(t *testing.T) {
	h := newTestHandler(t)
	_ = h.db.Close()
	w := do(t, h, "/prompt-snapshots?sha=sha_sys_1")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "internal server error") {
		t.Errorf("body should contain generic 'internal server error', got %s", w.Body.String())
	}
}

// /events bad cursor: mirror of TestListSessions_BadCursor — invalid
// base64 encoding and malformed two-field payload both 400.
func TestListEvents_BadCursor(t *testing.T) {
	h := newTestHandler(t)

	w := do(t, h, "/events?cursor=not-base64!!!")
	mustStatus(t, w, http.StatusBadRequest)
	if !strings.Contains(w.Body.String(), "cursor") {
		t.Errorf("body should mention cursor, got %s", w.Body.String())
	}

	// Malformed: valid base64 but only one field (no '|').
	w = do(t, h, "/events?cursor="+base64.RawURLEncoding.EncodeToString([]byte("only-one-field")))
	mustStatus(t, w, http.StatusBadRequest)
	if !strings.Contains(w.Body.String(), "cursor") {
		t.Errorf("body should mention cursor, got %s", w.Body.String())
	}
}

// --- GET /sessions/{id}/events — incremental tail polling ---
//
// Tail-poll endpoint contract: ascending seq order, seq itself is the
// cursor, idempotent overshoot, no filters, no next_cursor field. The
// tests below pin every clause of that contract — particularly the
// caught-up vs unknown-id distinction (200 empty vs 404) and the "no
// next_cursor field" negative assertion which locks the response shape.

func TestSessionEvents_HappyPathReturnsAllInSeqAsc(t *testing.T) {
	h := newTestHandler(t)
	w := do(t, h, "/sessions/sess_a/events")
	mustStatus(t, w, http.StatusOK)

	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 {
		t.Fatalf("got %d items, want 2; body = %s", len(resp.Items), w.Body.String())
	}
	if resp.Items[0].Seq != 1 || resp.Items[1].Seq != 2 {
		t.Errorf("seqs = [%d %d], want [1 2] (ASC)", resp.Items[0].Seq, resp.Items[1].Seq)
	}
	if resp.Items[0].ID != "evt_a1" || resp.Items[1].ID != "evt_a2" {
		t.Errorf("ids = [%q %q], want [evt_a1 evt_a2]", resp.Items[0].ID, resp.Items[1].ID)
	}
}

func TestSessionEvents_MidTailTrimsSeenPrefix(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events?since_seq=1")
	mustStatus(t, w, http.StatusOK)

	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].Seq != 2 {
		t.Errorf("got %+v, want one item with seq=2", seqsOf(resp.Items))
	}
}

// Caught-up poll keeps polling rather than resyncing — 200 with empty
// items, not 404. Critical for the 2s loop to survive idle gaps.
func TestSessionEvents_CaughtUpReturns200WithEmptyItems(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events?since_seq=2")
	mustStatus(t, w, http.StatusOK)

	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 {
		t.Errorf("got %d items, want 0 (caught up)", len(resp.Items))
	}
	// Empty array, not null, so the consumer's `items.length` works
	// without a null-guard.
	if !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Errorf("body should serialise empty items as `[]`, got %s", w.Body.String())
	}
}

// since_seq > max(seq) must NOT 404 — 404 means "stop polling", and the
// caller should be allowed to re-poll a stale cursor without resync.
func TestSessionEvents_OvershootIsIdempotent200(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events?since_seq=99")
	mustStatus(t, w, http.StatusOK)

	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 {
		t.Errorf("got %d items, want 0 (overshoot)", len(resp.Items))
	}
}

// Both sessions reuse seq=[1,2], so dropping the `WHERE session_id = ?`
// clause would still pass a happy-path test — this one would catch it.
func TestSessionEvents_SessionBoundaryNoCrossLeak(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events")
	mustStatus(t, w, http.StatusOK)
	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	for _, e := range resp.Items {
		if e.SessionID != "sess_a" {
			t.Errorf("got event with session_id=%q, want sess_a only", e.SessionID)
		}
	}
}

func TestSessionEvents_UnknownSessionReturns404(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_does_not_exist/events")
	mustStatus(t, w, http.StatusNotFound)
	if !strings.Contains(w.Body.String(), "session not found") {
		t.Errorf("body should say session not found, got %s", w.Body.String())
	}
}

func TestSessionEvents_BadSinceSeqReturns400(t *testing.T) {
	h := newTestHandler(t)
	for _, target := range []string{
		"/sessions/sess_a/events?since_seq=banana",
		"/sessions/sess_a/events?since_seq=-1",
		"/sessions/sess_a/events?since_seq=1.5",
	} {
		w := do(t, h, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body = %s", target, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "since_seq") {
			t.Errorf("%s: error body should name since_seq, got %s", target, w.Body.String())
		}
	}
}

// Param validation must run before the session-existence probe — an
// invalid limit on an unknown session must still 400 (not 404) so the
// client sees the consumer bug, not a misleading "missing session".
func TestSessionEvents_BadLimitReturns400(t *testing.T) {
	h := newTestHandler(t)

	w := do(t, h, "/sessions/sess_a/events?limit=0")
	mustStatus(t, w, http.StatusBadRequest)
	if !strings.Contains(w.Body.String(), "limit") {
		t.Errorf("body should name limit, got %s", w.Body.String())
	}

	w = do(t, h, "/sessions/sess_does_not_exist/events?limit=banana")
	mustStatus(t, w, http.StatusBadRequest)
}

func TestSessionEvents_BadIncludePayloadReturns400(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events?include_payload=banana")
	mustStatus(t, w, http.StatusBadRequest)
	if !strings.Contains(w.Body.String(), "include_payload") {
		t.Errorf("body should name include_payload, got %s", w.Body.String())
	}
}

func TestSessionEvents_IncludePayloadFalseOmitsPayload(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events?include_payload=false")
	mustStatus(t, w, http.StatusOK)

	// Negative JSON assertion — Event.Payload uses omitempty, so the key
	// disappears entirely when payload is empty.
	if strings.Contains(w.Body.String(), `"payload"`) {
		t.Errorf("payload key should be absent when include_payload=false; body = %s", w.Body.String())
	}
}

// items.length == limit is the burst signal: the client polls again
// immediately (skip the 2s wait) until the result drops below limit.
// This test exercises one burst round-trip; the loop is consumer-side.
func TestSessionEvents_LimitBoundaryAndBurstSemantics(t *testing.T) {
	h := newTestHandler(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := h.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	exec(`INSERT INTO sessions VALUES ('sess_burst','burst',NULL,'gpt-4o','{}','user_b','group_b','2024-05-19T00:00:00Z','2024-05-19T00:00:30Z')`)
	// Seed 30 events with seq=1..30. Insert in a non-sorted order to
	// ensure the ORDER BY clause is doing the work, not the insertion order.
	insertOrder := []int{15, 7, 1, 22, 30, 14, 3, 18, 9, 26, 5, 12, 20, 28, 2, 17, 24, 8, 19, 4, 21, 11, 27, 6, 13, 25, 10, 16, 29, 23}
	for _, n := range insertOrder {
		exec(`INSERT INTO session_events VALUES (?,?,?,?,?,NULL,0,?,?)`,
			fmt.Sprintf("evt_burst_%02d", n), "sess_burst", n,
			fmt.Sprintf("2024-05-19T00:00:%02dZ", n%60), "llm_response",
			`{"v":1}`, "2024-05-19T00:00:00Z")
	}

	// First poll: default limit=25, should return seq=1..25.
	w := do(t, h, "/sessions/sess_burst/events")
	mustStatus(t, w, http.StatusOK)
	var resp SessionEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 25 {
		t.Fatalf("page 1: got %d items, want 25 (default limit)", len(resp.Items))
	}
	// Ordering invariant.
	for i := 0; i < len(resp.Items)-1; i++ {
		if resp.Items[i].Seq >= resp.Items[i+1].Seq {
			t.Errorf("page 1 not strictly ASC at index %d: %d >= %d",
				i, resp.Items[i].Seq, resp.Items[i+1].Seq)
		}
	}
	if resp.Items[0].Seq != 1 || resp.Items[24].Seq != 25 {
		t.Errorf("page 1: seq range = [%d..%d], want [1..25]", resp.Items[0].Seq, resp.Items[24].Seq)
	}

	// Burst follow-up: cursor = max(items[].seq) = 25. Should drain the rest.
	maxSeq := resp.Items[len(resp.Items)-1].Seq
	w = do(t, h, fmt.Sprintf("/sessions/sess_burst/events?since_seq=%d", maxSeq))
	mustStatus(t, w, http.StatusOK)
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 5 {
		t.Fatalf("page 2: got %d items, want 5", len(resp.Items))
	}
	if resp.Items[0].Seq != 26 || resp.Items[4].Seq != 30 {
		t.Errorf("page 2: seq range = [%d..%d], want [26..30]",
			resp.Items[0].Seq, resp.Items[4].Seq)
	}
}

// Negative wire-assertion: no next_cursor field. seq is the cursor;
// an opaque token would invite consumers to round-trip one.
func TestSessionEvents_ResponseHasNoNextCursorField(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/events")
	mustStatus(t, w, http.StatusOK)
	if strings.Contains(w.Body.String(), "next_cursor") {
		t.Errorf("response must not contain next_cursor (seq is the cursor); body = %s", w.Body.String())
	}
}

// 500 must not leak DB-internal text. Mirrors TestGetSession_DBError.
func TestSessionEvents_DBError(t *testing.T) {
	h := newTestHandler(t)
	_ = h.db.Close()
	w := do(t, h, "/sessions/sess_a/events")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "internal server error") {
		t.Errorf("body should contain generic 'internal server error', got %s", w.Body.String())
	}
	for _, leak := range []string{"sql:", "sqlite", "database is closed"} {
		if strings.Contains(w.Body.String(), leak) {
			t.Errorf("body leaks internal error detail %q: %s", leak, w.Body.String())
		}
	}
}

func seqsOf(items []Event) []int {
	out := make([]int, len(items))
	for i, e := range items {
		out[i] = e.Seq
	}
	return out
}

func TestSessionDebugEvents_ReturnsVerbatimRowsInTimeOrder(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/sess_a/debug-events")
	mustStatus(t, w, http.StatusOK)

	var resp DebugEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	if len(resp.Items) != 3 {
		t.Fatalf("items = %d, want 3", len(resp.Items))
	}
	for i, want := range []string{"request", "response", "error"} {
		if resp.Items[i].Direction != want {
			t.Fatalf("items[%d].direction = %q, want %q (ts order)", i, resp.Items[i].Direction, want)
		}
	}
	// The streamed response is preserved verbatim as the SSE wire stream — NOT
	// reassembled into a single JSON object.
	if !strings.HasPrefix(resp.Items[1].Body, "data: {") {
		t.Fatalf("response body = %q, want raw SSE starting with 'data: {'", resp.Items[1].Body)
	}
	// The error row's body is a non-JSON diagnostic string, surfaced as-is.
	if resp.Items[2].Body != "Net::ReadTimeout: execution expired" {
		t.Fatalf("error body = %q, want the verbatim diagnostic", resp.Items[2].Body)
	}
}

func TestSessionDebugEvents_404OnUnknownSession(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/does-not-exist/debug-events")
	mustStatus(t, w, http.StatusNotFound)
}

func TestSessionDebugEvents_EmptyItemsWhenSessionHasNoDebugRows(t *testing.T) {
	// sess_b exists but has no ai_debug_events rows → 200 with an empty list,
	// distinct from the 404 of a non-existent session.
	w := do(t, newTestHandler(t), "/sessions/sess_b/debug-events")
	mustStatus(t, w, http.StatusOK)

	var resp DebugEventsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Items) != 0 {
		t.Fatalf("items = %d, want 0", len(resp.Items))
	}
}

func TestSessionDebugEvents_LimitValidation(t *testing.T) {
	h := newTestHandler(t)
	for _, target := range []string{
		"/sessions/sess_a/debug-events?limit=0",
		"/sessions/sess_a/debug-events?limit=-1",
		"/sessions/sess_a/debug-events?limit=501",
		"/sessions/sess_a/debug-events?limit=abc",
	} {
		mustStatus(t, do(t, h, target), http.StatusBadRequest)
	}
}

// annotateAssistantToolCalls pairs the n-th assistant message with the n-th
// MAIN-LOOP llm_response. Side-call llm_responses (confirmation classifier,
// title generation, tool-call repair corrector) are parented under an
// *_invoked sentinel event and have no assistant message row — they must be
// skipped, or every assistant message after the first side-call pairs with
// the wrong llm_response and its tool-call annotation shifts or disappears.
func TestAnnotateAssistantToolCalls_SkipsSideCallLLMResponses(t *testing.T) {
	msgs := []Message{
		{Seq: 1, Role: "user", Content: "set the responsible user"},
		{Seq: 2, Role: "assistant", Content: "calling the tool"},
		{Seq: 3, Role: "tool", Content: "ok"},
		{Seq: 4, Role: "assistant", Content: "done"},
	}
	evts := []Event{
		// Main-loop response #1: carries the tool call for assistant msg 2.
		{ID: "e1", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"tc-1"}]}`)},
		// Failed tool call → repair corrector side-call: sentinel + nested
		// llm_request/llm_response. No assistant message row exists for it.
		{ID: "e2", EventType: "tool_call_repair_invoked"},
		{ID: "e3", ParentID: "e2", EventType: "llm_request"},
		{ID: "e4", ParentID: "e2", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":null}`)},
		// Main-loop response #2: the final answer for assistant msg 4.
		{ID: "e5", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":null}`)},
	}
	annotateAssistantToolCalls(msgs, evts)

	if string(msgs[1].ToolCalls) != `[{"id":"tc-1"}]` {
		t.Fatalf("assistant msg 2 tool_calls = %q, want the main-loop annotation", msgs[1].ToolCalls)
	}
	// Without the sentinel skip, msg 4 would pair with the corrector's e4;
	// with it, msg 4 pairs with e5 (null → no tool_calls field).
	if msgs[3].ToolCalls != nil {
		t.Fatalf("assistant msg 4 tool_calls = %q, want none (final answer)", msgs[3].ToolCalls)
	}
}

// Session summarization is the one side-call whose sentinel event is NOT
// named `*_invoked`: the orchestrator parents the summarizer's
// llm_request/llm_response under a `summarization_triggered` event and
// writes no assistant message row. Its llm_response must be skipped too,
// or every assistant message after a summarization pairs with the wrong
// llm_response.
func TestAnnotateAssistantToolCalls_SkipsSummarizationLLMResponses(t *testing.T) {
	msgs := []Message{
		{Seq: 1, Role: "user", Content: "set the responsible user"},
		{Seq: 2, Role: "assistant", Content: "calling the tool"},
		{Seq: 3, Role: "tool", Content: "ok"},
		{Seq: 4, Role: "assistant", Content: "done"},
	}
	evts := []Event{
		// Main-loop response #1: carries the tool call for assistant msg 2.
		{ID: "e1", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"tc-1"}]}`)},
		// Session summarization side-call: `summarization_triggered`
		// sentinel + nested llm_request/llm_response. No assistant
		// message row exists for it. The noise payload makes a
		// mispairing observable (same trick as classifier-noise in
		// the ordinal-stability test below) — with null payloads on
		// both candidates the assertion could not tell e4 from e5.
		{ID: "e2", EventType: "summarization_triggered"},
		{ID: "e3", ParentID: "e2", EventType: "llm_request"},
		{ID: "e4", ParentID: "e2", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"summarizer-noise"}]}`)},
		// Main-loop response #2: the annotation assistant msg 4 must
		// receive.
		{ID: "e5", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"tc-2"}]}`)},
	}
	annotateAssistantToolCalls(msgs, evts)

	if string(msgs[1].ToolCalls) != `[{"id":"tc-1"}]` {
		t.Fatalf("assistant msg 2 tool_calls = %q, want the main-loop annotation", msgs[1].ToolCalls)
	}
	// Without the summarization sentinel, msg 4 pairs with the
	// summarizer's e4 and carries summarizer-noise instead.
	if string(msgs[3].ToolCalls) != `[{"id":"tc-2"}]` {
		t.Fatalf("assistant msg 4 tool_calls = %q, want tc-2 (not the summarizer's)", msgs[3].ToolCalls)
	}
}

// Regression guard for the ordinal shift itself: with a side-call response
// interleaved, the SECOND assistant message must still receive the SECOND
// main-loop response's annotation.
func TestAnnotateAssistantToolCalls_OrdinalStableAcrossSideCalls(t *testing.T) {
	msgs := []Message{
		{Seq: 1, Role: "assistant", Content: "first tool round"},
		{Seq: 2, Role: "assistant", Content: "second tool round"},
	}
	evts := []Event{
		{ID: "e1", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"tc-1"}]}`)},
		{ID: "e2", EventType: "confirmation_classification_invoked"},
		{ID: "e3", ParentID: "e2", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"classifier-noise"}]}`)},
		{ID: "e4", EventType: "llm_response",
			Payload: json.RawMessage(`{"native_tool_calls_raw":[{"id":"tc-2"}]}`)},
	}
	annotateAssistantToolCalls(msgs, evts)

	if string(msgs[0].ToolCalls) != `[{"id":"tc-1"}]` {
		t.Fatalf("assistant msg 1 tool_calls = %q, want tc-1", msgs[0].ToolCalls)
	}
	if string(msgs[1].ToolCalls) != `[{"id":"tc-2"}]` {
		t.Fatalf("assistant msg 2 tool_calls = %q, want tc-2 (not the side-call's)", msgs[1].ToolCalls)
	}
}

// titleItems decodes /sessions into just (id, title) pairs for the title tests.
func titleItems(t *testing.T, w *httptest.ResponseRecorder) []struct{ ID, Title string } {
	t.Helper()
	var r struct {
		Items []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		} `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		t.Fatalf("decode: %v; body=%s", err, w.Body.String())
	}
	out := make([]struct{ ID, Title string }, len(r.Items))
	for i, it := range r.Items {
		out[i] = struct{ ID, Title string }{it.ID, it.Title}
	}
	return out
}

func TestListSessions_FilterByTitle(t *testing.T) {
	h := newTestHandler(t)

	// Case-insensitive substring: sess_a has title "First chat title", sess_b
	// has a NULL title (never matches).
	w := do(t, h, "/sessions?q=first")
	mustStatus(t, w, http.StatusOK)
	if items := titleItems(t, w); len(items) != 1 || items[0].ID != "sess_a" {
		t.Fatalf("q=first: got %+v, want only sess_a", items)
	}

	// No match → empty.
	w = do(t, h, "/sessions?q=zzz-nope")
	mustStatus(t, w, http.StatusOK)
	if items := titleItems(t, w); len(items) != 0 {
		t.Fatalf("q=zzz-nope: got %d items, want 0", len(items))
	}

	// The LIKE metacharacters '%' (any run) and '_' (any single char) and the
	// escape char '\' must each match LITERALLY, not as wildcards. No seeded
	// title contains any of them, so an escaped search returns nothing; a leak
	// would make '%' or '_' match sess_a. (%25='%', %5F='_', %5C='\'.)
	for _, meta := range []string{"%25", "%5F", "%5C"} {
		w = do(t, h, "/sessions?q="+meta)
		mustStatus(t, w, http.StatusOK)
		if items := titleItems(t, w); len(items) != 0 {
			t.Fatalf("q=%s: LIKE metachar leaked, got %d items, want 0", meta, len(items))
		}
	}
}

func TestUpdateSessionTitle(t *testing.T) {
	h := newTestHandler(t)

	// Rename sess_a (which already has an auto-title) — the write is
	// unconditional, so the user title wins.
	w := doReq(t, h, "PATCH", "/sessions/sess_a", `{"title":"Renamed A"}`)
	mustStatus(t, w, http.StatusOK)

	// The new title is persisted and searchable.
	w = do(t, h, "/sessions?q=Renamed")
	mustStatus(t, w, http.StatusOK)
	if items := titleItems(t, w); len(items) != 1 || items[0].ID != "sess_a" || items[0].Title != "Renamed A" {
		t.Fatalf("after rename: got %+v, want sess_a titled 'Renamed A'", items)
	}

	// Whitespace-only title is rejected (empty after trim).
	mustStatus(t, doReq(t, h, "PATCH", "/sessions/sess_a", `{"title":"   "}`), http.StatusBadRequest)

	// Unknown session id → 404 (zero rows affected).
	mustStatus(t, doReq(t, h, "PATCH", "/sessions/does-not-exist", `{"title":"X"}`), http.StatusNotFound)

	// Malformed JSON → 400.
	mustStatus(t, doReq(t, h, "PATCH", "/sessions/sess_a", `{"title":`), http.StatusBadRequest)

	// Unknown field → 400 (DisallowUnknownFields).
	mustStatus(t, doReq(t, h, "PATCH", "/sessions/sess_a", `{"title":"ok","x":1}`), http.StatusBadRequest)

	// Over the rune cap → 400.
	mustStatus(t, doReq(t, h, "PATCH", "/sessions/sess_a",
		`{"title":"`+strings.Repeat("a", maxTitleLen+1)+`"}`), http.StatusBadRequest)
}

// The title search must never escape the caller's actor scope: q is ANDed with
// entity_id/group_id, so a client user searching only ever sees their OWN
// sessions. This is the tenant-isolation guarantee the Rails proxy relies on
// (it always passes the acting user's entity_id alongside q).
func TestListSessions_TitleSearchStaysWithinActorScope(t *testing.T) {
	h := newTestHandler(t)

	// sess_a ("First chat title") belongs to entity_id user_1; sess_b (NULL
	// title) to user_2. Scoped to user_2, a q=first search must NOT surface
	// user_1's matching session — q ANDs the scope, it is never a global trawl.
	w := do(t, h, "/sessions?entity_id=user_2&q=first")
	mustStatus(t, w, http.StatusOK)
	if items := titleItems(t, w); len(items) != 0 {
		t.Fatalf("cross-actor leak: user_2 q=first returned %+v, want none", items)
	}

	// Scoped to the owner it matches — confirms the AND is a scope, not an over-filter.
	w = do(t, h, "/sessions?entity_id=user_1&q=first")
	mustStatus(t, w, http.StatusOK)
	if items := titleItems(t, w); len(items) != 1 || items[0].ID != "sess_a" {
		t.Fatalf("owner scope: got %+v, want sess_a", items)
	}
}

// The two-pool split is the least-privilege guarantee: the read pool (OpenDB)
// must physically reject a write, while the write pool (OpenWriteDB) accepts it.
// Verified on the sqlite path, where read-only (mode=ro) is file-level and holds
// across the whole pool.
func TestOpenDB_ReadOnlyRejectsWrites(t *testing.T) {
	path := t.TempDir() + "/ro.db"

	// Seed via a throwaway read-write handle (default rollback journal), then close.
	seed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT)`,
		`INSERT INTO sessions VALUES ('s1','orig')`,
	} {
		if _, err := seed.Exec(q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	_ = seed.Close()

	// Read pool: rejects the write, still serves the read.
	ro, _, err := OpenDB("sqlite", path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer func() { _ = ro.Close() }()
	if _, err := ro.Exec(`UPDATE sessions SET title='hacked' WHERE id='s1'`); err == nil {
		t.Fatal("read pool accepted a write; want rejection")
	}
	var got string
	if err := ro.QueryRow(`SELECT title FROM sessions WHERE id='s1'`).Scan(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "orig" {
		t.Fatalf("read got %q, want orig (the rejected write must not have applied)", got)
	}

	// Write pool: accepts the write.
	wr, _, err := OpenWriteDB("sqlite", path)
	if err != nil {
		t.Fatalf("OpenWriteDB: %v", err)
	}
	defer func() { _ = wr.Close() }()
	if _, err := wr.Exec(`UPDATE sessions SET title='ok' WHERE id='s1'`); err != nil {
		t.Fatalf("write pool rejected a write: %v", err)
	}
}
