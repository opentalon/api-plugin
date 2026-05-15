package plugin

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/opentalon/api-plugin/config"
	pluginpkg "github.com/opentalon/opentalon/pkg/plugin"
)

// Handler implements the OpenTalon plugin interface and serves an HTTP REST API.
//
// The HTTP surface is the primary contract — see routes.go and README.md.
// The gRPC Execute path (the OpenTalon "plugin as LLM tool" entry point)
// is intentionally minimal: api-plugin's purpose is to be queried by the
// review UI, not by the LLM itself. Tool-style data access lives in the
// sibling mcp-plugin.
type Handler struct {
	db      *sql.DB
	dialect Dialect
}

// NewHandler creates an unconfigured handler. DB is opened in Configure().
func NewHandler() *Handler {
	return &Handler{}
}

// Capabilities advertises no tool actions — the api-plugin is read-only
// data infrastructure for the review UI, not a tool the LLM should call.
// Keeping the actions list empty (rather than removing the method) keeps
// the plugin handshake protocol-compliant.
func (h *Handler) Capabilities() pluginpkg.CapabilitiesMsg {
	return pluginpkg.CapabilitiesMsg{
		Name:        "api",
		Description: "REST API over OpenTalon sessions, session_events, and prompt_snapshots — consumed by the review UI",
		Actions:     nil,
	}
}

// Execute is a no-op: api-plugin doesn't expose tool actions to the
// orchestrator (see Capabilities). Returning an explicit error rather
// than a silent empty response surfaces misconfiguration loudly if some
// future routing change starts dispatching tool calls here.
func (h *Handler) Execute(req pluginpkg.Request) pluginpkg.Response {
	return pluginpkg.Response{
		CallID: req.ID,
		Error:  "api-plugin: no tool actions; query the HTTP REST API",
	}
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
