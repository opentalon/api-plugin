package plugin

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) (*sql.DB, Dialect) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}

	// Create schema matching core migrations 001-005.
	for _, ddl := range []string{
		`CREATE TABLE sessions (id TEXT PRIMARY KEY, messages TEXT, summary TEXT, active_model TEXT, metadata TEXT, entity_id TEXT DEFAULT '', group_id TEXT DEFAULT '', created_at TEXT, updated_at TEXT)`,
		`CREATE TABLE messages (session_id TEXT, seq INTEGER, role TEXT, content TEXT, created_at TEXT)`,
		`CREATE UNIQUE INDEX idx_messages_session_seq ON messages(session_id, seq)`,
		`CREATE TABLE memories (id TEXT PRIMARY KEY, actor_id TEXT, content TEXT, tags TEXT, created_at TEXT)`,
		`CREATE TABLE entities (id TEXT PRIMARY KEY, group_id TEXT, first_seen TEXT, last_seen TEXT)`,
		`CREATE TABLE profile_usage (id TEXT PRIMARY KEY, entity_id TEXT, group_id TEXT, channel_id TEXT, session_id TEXT, model_id TEXT, input_tokens INTEGER, output_tokens INTEGER, tool_calls INTEGER, input_cost REAL, output_cost REAL, created_at TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("DDL: %v", err)
		}
	}

	// Seed data.
	db.Exec(`INSERT INTO sessions VALUES ('u1:ws:room1','[]','old summary','claude','{}','u1','team-a','2024-01-01T00:00:00Z','2024-01-02T00:00:00Z')`)
	db.Exec(`INSERT INTO sessions VALUES ('u2:ws:room2','[]','','claude','{}','u2','team-b','2024-01-01T00:00:00Z','2024-01-01T12:00:00Z')`)
	for i := 1; i <= 6; i++ {
		db.Exec(`INSERT INTO messages VALUES ('u1:ws:room1',?,'user',?,?)`, i*2-1, "question "+string(rune('A'-1+i)), "2024-01-01T00:00:00Z")
		db.Exec(`INSERT INTO messages VALUES ('u1:ws:room1',?,'assistant',?,?)`, i*2, "answer "+string(rune('A'-1+i)), "2024-01-01T00:00:00Z")
	}
	db.Exec(`INSERT INTO messages VALUES ('u2:ws:room2',1,'user','hi','2024-01-01T00:00:00Z')`)
	db.Exec(`INSERT INTO messages VALUES ('u2:ws:room2',2,'assistant','hello','2024-01-01T00:00:00Z')`)

	db.Exec(`INSERT INTO memories VALUES ('m1','u1','remember this','["work"]','2024-01-01T00:00:00Z')`)
	db.Exec(`INSERT INTO memories VALUES ('m2','','general fact','["general"]','2024-01-01T00:00:00Z')`)

	db.Exec(`INSERT INTO entities VALUES ('u1','team-a','2024-01-01T00:00:00Z','2024-01-02T00:00:00Z')`)
	db.Exec(`INSERT INTO entities VALUES ('u2','team-b','2024-01-01T00:00:00Z','2024-01-01T12:00:00Z')`)

	db.Exec(`INSERT INTO profile_usage VALUES ('usg1','u1','team-a','ws','u1:ws:room1','claude',100,50,2,0.01,0.005,'2024-01-01T00:00:00Z')`)
	db.Exec(`INSERT INTO profile_usage VALUES ('usg2','u1','team-a','ws','u1:ws:room1','claude',200,100,3,0.02,0.01,'2024-01-02T00:00:00Z')`)

	t.Cleanup(func() { _ = db.Close() })
	return db, sqliteDialect
}

func newTestHandler(t *testing.T) *Handler {
	db, dialect := setupTestDB(t)
	return &Handler{db: db, dialect: dialect}
}

func TestHealth(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleHealth(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestListSessions(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleListSessions(w, httptest.NewRequest("GET", "/sessions", nil))
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var items []SessionListItem
	json.Unmarshal(w.Body.Bytes(), &items)
	if len(items) != 2 {
		t.Fatalf("got %d sessions, want 2", len(items))
	}
}

func TestListSessions_FilterByEntity(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleListSessions(w, httptest.NewRequest("GET", "/sessions?entity=u1", nil))
	var items []SessionListItem
	json.Unmarshal(w.Body.Bytes(), &items)
	if len(items) != 1 || items[0].EntityID != "u1" {
		t.Fatalf("got %+v, want 1 session for u1", items)
	}
}

func TestGetSession(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest("GET", "/sessions/u1:ws:room1", nil)
	r.SetPathValue("id", "u1:ws:room1")
	w := httptest.NewRecorder()
	h.handleGetSession(w, r)
	if w.Code != 200 {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var sess SessionDetail
	json.Unmarshal(w.Body.Bytes(), &sess)
	if len(sess.Messages) != 12 {
		t.Errorf("got %d messages, want 12", len(sess.Messages))
	}
}

func TestSessionMessages_Last3(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest("GET", "/sessions/u1:ws:room1/messages?last=3", nil)
	r.SetPathValue("id", "u1:ws:room1")
	w := httptest.NewRecorder()
	h.handleSessionMessages(w, r)
	var msgs []Message
	json.Unmarshal(w.Body.Bytes(), &msgs)
	if len(msgs) != 6 { // 3 pairs
		t.Fatalf("got %d messages, want 6 (3 pairs)", len(msgs))
	}
	// Should be oldest first after reversal.
	if msgs[0].Role != "user" {
		t.Errorf("first message role = %q, want user", msgs[0].Role)
	}
}

func TestMessages_ByEntity(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleMessages(w, httptest.NewRequest("GET", "/messages?entity=u1&last=2", nil))
	var msgs []Message
	json.Unmarshal(w.Body.Bytes(), &msgs)
	if len(msgs) != 4 { // 2 pairs
		t.Fatalf("got %d messages, want 4 (2 pairs)", len(msgs))
	}
	for _, m := range msgs {
		if m.SessionID != "u1:ws:room1" {
			t.Errorf("unexpected session_id = %q", m.SessionID)
		}
	}
}

func TestListMemories(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleListMemories(w, httptest.NewRequest("GET", "/memories", nil))
	var mems []Memory
	json.Unmarshal(w.Body.Bytes(), &mems)
	if len(mems) != 2 {
		t.Fatalf("got %d memories, want 2", len(mems))
	}
}

func TestListMemories_FilterByTag(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleListMemories(w, httptest.NewRequest("GET", "/memories?tag=work", nil))
	var mems []Memory
	json.Unmarshal(w.Body.Bytes(), &mems)
	if len(mems) != 1 || mems[0].ID != "m1" {
		t.Fatalf("got %+v, want 1 memory with tag work", mems)
	}
}

func TestListEntities(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleListEntities(w, httptest.NewRequest("GET", "/entities?group=team-a", nil))
	var ents []Entity
	json.Unmarshal(w.Body.Bytes(), &ents)
	if len(ents) != 1 || ents[0].ID != "u1" {
		t.Fatalf("got %+v, want 1 entity in team-a", ents)
	}
}

func TestUsageSummary(t *testing.T) {
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	h.handleUsageSummary(w, httptest.NewRequest("GET", "/usage/summary?entity=u1&group_by=model_id", nil))
	var items []UsageSummaryItem
	json.Unmarshal(w.Body.Bytes(), &items)
	if len(items) != 1 {
		t.Fatalf("got %d summary items, want 1", len(items))
	}
	if items[0].TotalInput != 300 || items[0].TotalOutput != 150 {
		t.Errorf("totals = %d/%d, want 300/150", items[0].TotalInput, items[0].TotalOutput)
	}
}

func TestAuthMiddleware(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})

	// With token required.
	h := authMiddleware("secret", inner)

	// No token → 401.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 401 {
		t.Errorf("no token: status = %d, want 401", w.Code)
	}

	// Wrong token → 401.
	w = httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Errorf("wrong token: status = %d, want 401", w.Code)
	}

	// Correct token → 200.
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Errorf("correct token: status = %d, want 200", w.Code)
	}

	// No token configured → pass through.
	h2 := authMiddleware("", inner)
	w = httptest.NewRecorder()
	h2.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 {
		t.Errorf("no auth config: status = %d, want 200", w.Code)
	}
}
