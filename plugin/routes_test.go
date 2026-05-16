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
			active_model TEXT,
			metadata TEXT,
			entity_id TEXT DEFAULT '',
			group_id TEXT DEFAULT '',
			created_at TEXT,
			updated_at TEXT
		)`,
		`CREATE TABLE messages (session_id TEXT, seq INTEGER, role TEXT, content TEXT, created_at TEXT)`,
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
		`CREATE TABLE prompt_snapshots (
			sha256 TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
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
	exec(`INSERT INTO sessions VALUES ('sess_a','first session','gpt-4o','{}','user_1','group_x','2024-01-01T10:00:00Z','2024-01-01T10:30:00Z')`)
	exec(`INSERT INTO sessions VALUES ('sess_b','second session','gpt-4o','{"locale":"de"}','user_2','group_y','2024-02-01T10:00:00Z','2024-02-01T10:30:00Z')`)

	exec(`INSERT INTO messages VALUES ('sess_a',1,'user','hi','2024-01-01T10:00:00Z')`)
	exec(`INSERT INTO messages VALUES ('sess_a',2,'assistant','hello','2024-01-01T10:00:01Z')`)

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
	return &Handler{db: db, dialect: dialect}
}

func do(t *testing.T, h *Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest("GET", target, nil))
	return w
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
	for i := 0; i < maxExcludeEntityIDs+1; i++ {
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

func TestGetSession_NotFound(t *testing.T) {
	w := do(t, newTestHandler(t), "/sessions/does_not_exist")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
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
		`INSERT INTO sessions VALUES ('sess_c','third','gpt-4o','{}','user_3','group_z','2024-03-01T10:00:00Z','2024-03-01T10:30:00Z')`); err != nil {
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
			`INSERT INTO sessions VALUES (?,'tie','gpt-4o','{}','user_tie','group_tie',?,?)`,
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
