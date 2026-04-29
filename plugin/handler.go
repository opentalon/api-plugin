package plugin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/opentalon/api-plugin/config"
	pluginpkg "github.com/opentalon/opentalon/pkg/plugin"
)

// Handler implements the OpenTalon plugin interface and serves an HTTP REST API.
type Handler struct {
	db      *sql.DB
	dialect Dialect
}

// NewHandler creates an unconfigured handler. DB is opened in Configure().
func NewHandler() *Handler {
	return &Handler{}
}

// Capabilities returns the plugin's declared actions.
func (h *Handler) Capabilities() pluginpkg.CapabilitiesMsg {
	return pluginpkg.CapabilitiesMsg{
		Name:        "api",
		Description: "REST API for querying OpenTalon data (sessions, messages, memories, entities, usage)",
		Actions: []pluginpkg.ActionMsg{
			{
				Name:        "query",
				Description: "Query stored data via the API plugin",
				Parameters: []pluginpkg.ParameterMsg{
					{Name: "resource", Description: "Resource: sessions, messages, memories, entities, usage", Type: "string", Required: true},
					{Name: "id", Description: "Optional resource ID", Type: "string"},
					{Name: "filter", Description: "Optional JSON filter (entity, group, last, etc.)", Type: "string"},
				},
			},
		},
	}
}

// Execute handles gRPC tool calls.
func (h *Handler) Execute(req pluginpkg.Request) pluginpkg.Response {
	if h.db == nil {
		return pluginpkg.Response{CallID: req.ID, Error: "database not configured"}
	}
	resource := req.Args["resource"]
	id := req.Args["id"]
	filterJSON := req.Args["filter"]

	var filter map[string]string
	if filterJSON != "" {
		_ = json.Unmarshal([]byte(filterJSON), &filter)
	}

	var result string
	var err error
	switch resource {
	case "sessions":
		if id != "" {
			result, err = h.querySessionByID(id)
		} else {
			result, err = h.querySessionsList(filter)
		}
	case "messages":
		result, err = h.queryMessages(id, filter)
	case "memories":
		result, err = h.queryMemories(id, filter)
	case "entities":
		result, err = h.queryEntities(id, filter)
	case "usage":
		result, err = h.queryUsage(filter)
	default:
		return pluginpkg.Response{CallID: req.ID, Error: fmt.Sprintf("unknown resource: %s", resource)}
	}
	if err != nil {
		return pluginpkg.Response{CallID: req.ID, Error: err.Error()}
	}
	return pluginpkg.Response{CallID: req.ID, Content: result}
}

// Configure opens the database and starts the HTTP server.
func (h *Handler) Configure(configJSON string) error {
	cfg, err := config.Parse(configJSON)
	if err != nil {
		return err
	}
	if cfg.DBDSN == "" {
		return fmt.Errorf("api-plugin: no database DSN configured (missing __db_dsn)")
	}

	db, dialect, err := OpenDB(cfg.DBDriver, cfg.DBDSN)
	if err != nil {
		return fmt.Errorf("api-plugin: %w", err)
	}
	h.db = db
	h.dialect = dialect

	// Start HTTP server if OPENTALON_HTTP_PORT is set.
	if port := os.Getenv("OPENTALON_HTTP_PORT"); port != "" {
		addr := "127.0.0.1:" + port
		mux := h.routes()
		handler := authMiddleware(cfg.APIToken, mux)
		go func() {
			log.Printf("api-plugin: HTTP server on %s", addr)
			if err := http.ListenAndServe(addr, handler); err != nil {
				log.Printf("api-plugin: HTTP server error: %v", err)
			}
		}()
	}

	log.Printf("api-plugin: configured (driver=%s)", cfg.DBDriver)
	return nil
}

// gRPC query helpers — return JSON strings.

func (h *Handler) querySessionByID(id string) (string, error) {
	sess, err := getSession(h.db, h.dialect, id)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(sess)
	return string(b), nil
}

func (h *Handler) querySessionsList(filter map[string]string) (string, error) {
	params := listParams(filter)
	sessions, err := listSessions(h.db, h.dialect, filter["entity"], filter["group"], params)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(sessions)
	return string(b), nil
}

func (h *Handler) queryMessages(sessionID string, filter map[string]string) (string, error) {
	params := listParams(filter)
	msgs, err := listMessages(h.db, h.dialect, sessionID, filter["entity"], filter["group"], filter["roles"], params)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(msgs)
	return string(b), nil
}

func (h *Handler) queryMemories(_ string, filter map[string]string) (string, error) {
	params := listParams(filter)
	mems, err := listMemories(h.db, h.dialect, filter["actor_id"], filter["tag"], filter["q"], params)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(mems)
	return string(b), nil
}

func (h *Handler) queryEntities(_ string, filter map[string]string) (string, error) {
	params := listParams(filter)
	ents, err := listEntities(h.db, h.dialect, filter["group"], params)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(ents)
	return string(b), nil
}

func (h *Handler) queryUsage(filter map[string]string) (string, error) {
	params := listParams(filter)
	records, err := listUsage(h.db, h.dialect, filter["entity"], filter["group"], filter["session"], filter["since"], params)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(records)
	return string(b), nil
}
