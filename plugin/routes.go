package plugin

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// routes registers all HTTP handlers.
func (h *Handler) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /sessions", h.handleListSessions)
	mux.HandleFunc("GET /sessions/{id}", h.handleGetSession)
	mux.HandleFunc("GET /sessions/{id}/messages", h.handleSessionMessages)
	mux.HandleFunc("GET /messages", h.handleMessages)
	mux.HandleFunc("GET /memories", h.handleListMemories)
	mux.HandleFunc("GET /memories/{id}", h.handleGetMemory)
	mux.HandleFunc("GET /entities", h.handleListEntities)
	mux.HandleFunc("GET /entities/{id}", h.handleGetEntity)
	mux.HandleFunc("GET /usage", h.handleListUsage)
	mux.HandleFunc("GET /usage/summary", h.handleUsageSummary)
	return mux
}

// --- JSON response types ---

type SessionListItem struct {
	ID           string `json:"id"`
	EntityID     string `json:"entity_id,omitempty"`
	GroupID      string `json:"group_id,omitempty"`
	Summary      string `json:"summary,omitempty"`
	ActiveModel  string `json:"active_model,omitempty"`
	MessageCount int    `json:"message_count"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type SessionDetail struct {
	ID          string            `json:"id"`
	EntityID    string            `json:"entity_id,omitempty"`
	GroupID     string            `json:"group_id,omitempty"`
	Summary     string            `json:"summary,omitempty"`
	ActiveModel string            `json:"active_model,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Messages    []Message         `json:"messages"`
	CreatedAt   string            `json:"created_at"`
	UpdatedAt   string            `json:"updated_at"`
}

type Message struct {
	SessionID string `json:"session_id,omitempty"`
	Seq       int    `json:"seq"`
	Role      string `json:"role"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

type Memory struct {
	ID        string   `json:"id"`
	ActorID   string   `json:"actor_id,omitempty"`
	Content   string   `json:"content"`
	Tags      []string `json:"tags"`
	CreatedAt string   `json:"created_at"`
}

type Entity struct {
	ID        string `json:"id"`
	GroupID   string `json:"group_id,omitempty"`
	FirstSeen string `json:"first_seen"`
	LastSeen  string `json:"last_seen"`
}

type UsageRecord struct {
	ID           string  `json:"id"`
	EntityID     string  `json:"entity_id"`
	GroupID      string  `json:"group_id,omitempty"`
	ChannelID    string  `json:"channel_id"`
	SessionID    string  `json:"session_id"`
	ModelID      string  `json:"model_id"`
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	ToolCalls    int     `json:"tool_calls"`
	InputCost    float64 `json:"input_cost"`
	OutputCost   float64 `json:"output_cost"`
	CreatedAt    string  `json:"created_at"`
}

type UsageSummaryItem struct {
	GroupBy      string  `json:"group_by_value"`
	TotalInput   int     `json:"total_input_tokens"`
	TotalOutput  int     `json:"total_output_tokens"`
	TotalCalls   int     `json:"total_tool_calls"`
	TotalInCost  float64 `json:"total_input_cost"`
	TotalOutCost float64 `json:"total_output_cost"`
	Count        int     `json:"count"`
}

type Pagination struct {
	Limit  int
	Offset int
}

func listParams(m map[string]string) Pagination {
	p := Pagination{Limit: 20, Offset: 0}
	if m == nil {
		return p
	}
	if v, err := strconv.Atoi(m["limit"]); err == nil && v > 0 && v <= 100 {
		p.Limit = v
	}
	if v, err := strconv.Atoi(m["offset"]); err == nil && v >= 0 {
		p.Offset = v
	}
	return p
}

func paginationFromQuery(r *http.Request) Pagination {
	p := Pagination{Limit: 20, Offset: 0}
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 100 {
		p.Limit = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
		p.Offset = v
	}
	return p
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
		writeErr(w, 503, "db unreachable")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	p := paginationFromQuery(r)
	entity := r.URL.Query().Get("entity")
	group := r.URL.Query().Get("group")
	sessions, err := listSessions(h.db, h.dialect, entity, group, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, sessions)
}

func (h *Handler) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, err := getSession(h.db, h.dialect, id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, sess)
}

func (h *Handler) handleSessionMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p := paginationFromQuery(r)
	if v, err := strconv.Atoi(r.URL.Query().Get("last")); err == nil && v > 0 {
		p.Limit = v * 2 // pairs
	}
	roles := r.URL.Query().Get("roles")
	msgs, err := listMessages(h.db, h.dialect, id, "", "", roles, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, msgs)
}

func (h *Handler) handleMessages(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")
	group := r.URL.Query().Get("group")
	roles := r.URL.Query().Get("roles")
	p := paginationFromQuery(r)
	if v, err := strconv.Atoi(r.URL.Query().Get("last")); err == nil && v > 0 {
		p.Limit = v * 2 // pairs
	}
	msgs, err := listMessages(h.db, h.dialect, "", entity, group, roles, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, msgs)
}

func (h *Handler) handleListMemories(w http.ResponseWriter, r *http.Request) {
	p := paginationFromQuery(r)
	actorID := r.URL.Query().Get("actor_id")
	tag := r.URL.Query().Get("tag")
	q := r.URL.Query().Get("q")
	mems, err := listMemories(h.db, h.dialect, actorID, tag, q, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, mems)
}

func (h *Handler) handleGetMemory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mem, err := getMemory(h.db, h.dialect, id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, mem)
}

func (h *Handler) handleListEntities(w http.ResponseWriter, r *http.Request) {
	p := paginationFromQuery(r)
	group := r.URL.Query().Get("group")
	ents, err := listEntities(h.db, h.dialect, group, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, ents)
}

func (h *Handler) handleGetEntity(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ent, err := getEntity(h.db, h.dialect, id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, ent)
}

func (h *Handler) handleListUsage(w http.ResponseWriter, r *http.Request) {
	p := paginationFromQuery(r)
	entity := r.URL.Query().Get("entity")
	group := r.URL.Query().Get("group")
	session := r.URL.Query().Get("session")
	since := r.URL.Query().Get("since")
	records, err := listUsage(h.db, h.dialect, entity, group, session, since, p)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, records)
}

func (h *Handler) handleUsageSummary(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")
	group := r.URL.Query().Get("group")
	since := r.URL.Query().Get("since")
	groupBy := r.URL.Query().Get("group_by")
	if groupBy == "" {
		groupBy = "model_id"
	}
	items, err := usageSummary(h.db, h.dialect, entity, group, since, groupBy)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, items)
}

// --- Query functions ---

func listSessions(db *sql.DB, d Dialect, entity, group string, p Pagination) ([]SessionListItem, error) {
	q := `SELECT s.id, COALESCE(s.entity_id,''), COALESCE(s.group_id,''), COALESCE(s.summary,''), COALESCE(s.active_model,''), (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id), s.created_at, s.updated_at FROM sessions s WHERE 1=1`
	var args []any
	if entity != "" {
		q += " AND s.entity_id = ?"
		args = append(args, entity)
	}
	if group != "" {
		q += " AND s.group_id = ?"
		args = append(args, group)
	}
	q += " ORDER BY s.updated_at DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := db.Query(d.Rebind(q), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []SessionListItem
	for rows.Next() {
		var s SessionListItem
		if err := rows.Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Summary, &s.ActiveModel, &s.MessageCount, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	if items == nil {
		items = []SessionListItem{}
	}
	return items, rows.Err()
}

func getSession(db *sql.DB, d Dialect, id string) (*SessionDetail, error) {
	var s SessionDetail
	var metadataJSON string
	err := db.QueryRow(
		d.Rebind(`SELECT id, COALESCE(entity_id,''), COALESCE(group_id,''), COALESCE(summary,''), COALESCE(active_model,''), COALESCE(metadata,'{}'), created_at, updated_at FROM sessions WHERE id = ?`),
		id,
	).Scan(&s.ID, &s.EntityID, &s.GroupID, &s.Summary, &s.ActiveModel, &metadataJSON, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(metadataJSON), &s.Metadata)

	rows, err := db.Query(d.Rebind(`SELECT seq, role, content, created_at FROM messages WHERE session_id = ? ORDER BY seq`), id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.Seq, &m.Role, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		s.Messages = append(s.Messages, m)
	}
	if s.Messages == nil {
		s.Messages = []Message{}
	}
	return &s, rows.Err()
}

func listMessages(db *sql.DB, d Dialect, sessionID, entity, group, roles string, p Pagination) ([]Message, error) {
	q := `SELECT m.session_id, m.seq, m.role, m.content, m.created_at FROM messages m`
	var args []any
	needJoin := entity != "" || group != ""
	if needJoin {
		q += ` JOIN sessions s ON s.id = m.session_id`
	}
	q += ` WHERE 1=1`
	if sessionID != "" {
		q += " AND m.session_id = ?"
		args = append(args, sessionID)
	}
	if entity != "" {
		q += " AND s.entity_id = ?"
		args = append(args, entity)
	}
	if group != "" {
		q += " AND s.group_id = ?"
		args = append(args, group)
	}
	if roles != "" {
		roleList := strings.Split(roles, ",")
		placeholders := make([]string, len(roleList))
		for i, r := range roleList {
			placeholders[i] = "?"
			args = append(args, strings.TrimSpace(r))
		}
		q += " AND m.role IN (" + strings.Join(placeholders, ",") + ")"
	} else {
		// Default: user + assistant only.
		q += " AND m.role IN ('user', 'assistant')"
	}
	q += " ORDER BY m.session_id, m.seq DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := db.Query(d.Rebind(q), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.SessionID, &m.Seq, &m.Role, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	if msgs == nil {
		msgs = []Message{}
	}
	// Reverse so oldest first (we selected DESC for "last N").
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, rows.Err()
}

func listMemories(db *sql.DB, d Dialect, actorID, tag, q string, p Pagination) ([]Memory, error) {
	query := `SELECT id, COALESCE(actor_id,''), content, tags, created_at FROM memories WHERE 1=1`
	var args []any
	if actorID != "" {
		query += " AND actor_id = ?"
		args = append(args, actorID)
	}
	if tag != "" {
		query += " AND " + d.TagMatch("tags")
		args = append(args, tag)
	}
	if q != "" {
		query += " AND LOWER(content) LIKE ?"
		args = append(args, "%"+strings.ToLower(q)+"%")
	}
	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := db.Query(d.Rebind(query), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var mems []Memory
	for rows.Next() {
		var m Memory
		var tagsJSON string
		if err := rows.Scan(&m.ID, &m.ActorID, &m.Content, &tagsJSON, &m.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(tagsJSON), &m.Tags)
		if m.Tags == nil {
			m.Tags = []string{}
		}
		mems = append(mems, m)
	}
	if mems == nil {
		mems = []Memory{}
	}
	return mems, rows.Err()
}

func getMemory(db *sql.DB, d Dialect, id string) (*Memory, error) {
	var m Memory
	var tagsJSON string
	err := db.QueryRow(
		d.Rebind(`SELECT id, COALESCE(actor_id,''), content, tags, created_at FROM memories WHERE id = ?`), id,
	).Scan(&m.ID, &m.ActorID, &m.Content, &tagsJSON, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(tagsJSON), &m.Tags)
	if m.Tags == nil {
		m.Tags = []string{}
	}
	return &m, nil
}

func listEntities(db *sql.DB, d Dialect, group string, p Pagination) ([]Entity, error) {
	q := `SELECT id, COALESCE(group_id,''), first_seen, last_seen FROM entities WHERE 1=1`
	var args []any
	if group != "" {
		q += " AND group_id = ?"
		args = append(args, group)
	}
	q += " ORDER BY last_seen DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := db.Query(d.Rebind(q), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var ents []Entity
	for rows.Next() {
		var e Entity
		if err := rows.Scan(&e.ID, &e.GroupID, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, err
		}
		ents = append(ents, e)
	}
	if ents == nil {
		ents = []Entity{}
	}
	return ents, rows.Err()
}

func getEntity(db *sql.DB, d Dialect, id string) (*Entity, error) {
	var e Entity
	err := db.QueryRow(
		d.Rebind(`SELECT id, COALESCE(group_id,''), first_seen, last_seen FROM entities WHERE id = ?`), id,
	).Scan(&e.ID, &e.GroupID, &e.FirstSeen, &e.LastSeen)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func listUsage(db *sql.DB, d Dialect, entity, group, session, since string, p Pagination) ([]UsageRecord, error) {
	q := `SELECT id, entity_id, COALESCE(group_id,''), channel_id, session_id, COALESCE(model_id,''), input_tokens, output_tokens, tool_calls, input_cost, output_cost, created_at FROM profile_usage WHERE 1=1`
	var args []any
	if entity != "" {
		q += " AND entity_id = ?"
		args = append(args, entity)
	}
	if group != "" {
		q += " AND group_id = ?"
		args = append(args, group)
	}
	if session != "" {
		q += " AND session_id = ?"
		args = append(args, session)
	}
	if since != "" {
		q += " AND created_at >= ?"
		args = append(args, since)
	}
	q += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := db.Query(d.Rebind(q), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		if err := rows.Scan(&r.ID, &r.EntityID, &r.GroupID, &r.ChannelID, &r.SessionID, &r.ModelID, &r.InputTokens, &r.OutputTokens, &r.ToolCalls, &r.InputCost, &r.OutputCost, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	if records == nil {
		records = []UsageRecord{}
	}
	return records, rows.Err()
}

func usageSummary(db *sql.DB, d Dialect, entity, group, since, groupBy string) ([]UsageSummaryItem, error) {
	// Whitelist group_by column to prevent SQL injection.
	allowed := map[string]bool{"model_id": true, "channel_id": true, "group_id": true, "entity_id": true}
	if !allowed[groupBy] {
		groupBy = "model_id"
	}

	q := `SELECT COALESCE(` + groupBy + `,''), SUM(input_tokens), SUM(output_tokens), SUM(tool_calls), SUM(input_cost), SUM(output_cost), COUNT(*) FROM profile_usage WHERE 1=1`
	var args []any
	if entity != "" {
		q += " AND entity_id = ?"
		args = append(args, entity)
	}
	if group != "" {
		q += " AND group_id = ?"
		args = append(args, group)
	}
	if since != "" {
		q += " AND created_at >= ?"
		args = append(args, since)
	}
	q += " GROUP BY " + groupBy + " ORDER BY SUM(input_tokens + output_tokens) DESC"

	rows, err := db.Query(d.Rebind(q), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []UsageSummaryItem
	for rows.Next() {
		var s UsageSummaryItem
		if err := rows.Scan(&s.GroupBy, &s.TotalInput, &s.TotalOutput, &s.TotalCalls, &s.TotalInCost, &s.TotalOutCost, &s.Count); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	if items == nil {
		items = []UsageSummaryItem{}
	}
	return items, rows.Err()
}
