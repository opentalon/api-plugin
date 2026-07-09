package plugin

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

// Dialect encapsulates SQL differences between SQLite and PostgreSQL.
type Dialect struct{ name string }

var (
	sqliteDialect   = Dialect{"sqlite"}
	postgresDialect = Dialect{"postgres"}
)

// Rebind converts ? placeholders to $1, $2, … for PostgreSQL; no-op for SQLite.
func (d Dialect) Rebind(query string) string {
	if d.name != "postgres" {
		return query
	}
	var b strings.Builder
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// TagMatch returns dialect-specific SQL for "JSON array column contains value".
func (d Dialect) TagMatch(column string) string {
	if d.name == "postgres" {
		return fmt.Sprintf("EXISTS (SELECT 1 FROM json_array_elements_text(%s::json) AS _t WHERE _t = ?)", column)
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM json_each(%s) WHERE json_each.value = ?)", column)
}

// JSONInt returns dialect-specific SQL that extracts a top-level integer
// field from a JSON-encoded TEXT column. Used by JIT aggregation queries
// over session_events.payload — opentalon stores payload as TEXT for
// schema portability (see migration 009_session_events.sql), so analytics
// joins extract on the read path here.
//
// fieldName MUST be a hard-coded constant (event_types.go field names)
// — never user input — because it's interpolated rather than bound. The
// helpers in this file follow that contract.
func (d Dialect) JSONInt(column, fieldName string) string {
	if d.name == "postgres" {
		return fmt.Sprintf(`COALESCE(NULLIF(%s::jsonb->>'%s','')::bigint, 0)`, column, fieldName)
	}
	return fmt.Sprintf(`COALESCE(CAST(json_extract(%s,'$.%s') AS INTEGER), 0)`, column, fieldName)
}

// JSONFloat is the float64 counterpart of JSONInt — for cost_input /
// cost_output stamped on llm_response payloads. Same fieldName-as-constant
// contract applies.
func (d Dialect) JSONFloat(column, fieldName string) string {
	if d.name == "postgres" {
		return fmt.Sprintf(`COALESCE(NULLIF(%s::jsonb->>'%s','')::double precision, 0)`, column, fieldName)
	}
	return fmt.Sprintf(`COALESCE(CAST(json_extract(%s,'$.%s') AS REAL), 0)`, column, fieldName)
}

// DateBucket returns a dialect-specific SQL fragment that truncates an
// RFC3339 timestamp TEXT column to a UTC-anchored bucket-start date,
// formatted as YYYY-MM-DD text. Used by /events/stats?bucket_by=… to
// group events into day / week / month / year buckets in a portable way.
//
// granularity MUST be one of "day", "week", "month", "year" — validated
// upstream in eventsStatsOptsFromQuery so the value can be interpolated
// safely (it is never user-controlled past validation).
//
// Bucket-key semantics:
//   - day   → that day's date
//   - week  → Monday of that week (ISO 8601, aligned with PG date_trunc)
//   - month → first of that month
//   - year  → January 1 of that year
func (d Dialect) DateBucket(column, granularity string) string {
	if d.name == "postgres" {
		return fmt.Sprintf(`to_char(date_trunc('%s', %s::timestamptz AT TIME ZONE 'UTC'), 'YYYY-MM-DD')`,
			granularity, column)
	}
	switch granularity {
	case "day":
		return fmt.Sprintf("date(%s)", column)
	case "week":
		// 'weekday 0' = next Sunday (or same day if already Sunday);
		// '-6 days' then yields the ISO-Monday of that week.
		return fmt.Sprintf("date(%s, 'weekday 0', '-6 days')", column)
	case "month":
		return fmt.Sprintf("date(%s, 'start of month')", column)
	case "year":
		return fmt.Sprintf("date(%s, 'start of year')", column)
	}
	return ""
}

// OpenDB opens the read pool every query endpoint uses. It requests read-only
// mode as defense-in-depth (sqlite: file-level mode=ro, robust across the whole
// pool; postgres: a per-connection SET, best-effort — see openDB). The real
// guarantee is at the CODE layer: query handlers only ever issue SELECTs, and
// the sole writer is a separate pool (OpenWriteDB).
func OpenDB(driver, dsn string) (*sql.DB, Dialect, error) {
	return openDB(driver, dsn, true)
}

// OpenWriteDB opens a read-WRITE pool on the SAME database — core's own primary
// (the DSN is injected by the host, so the role already has write grants;
// api-plugin just declines them everywhere else). Exactly one handler uses this
// pool — PATCH /sessions/{id}, writing only the title column — so no read path
// can reach a mutating connection.
func OpenWriteDB(driver, dsn string) (*sql.DB, Dialect, error) {
	return openDB(driver, dsn, false)
}

func openDB(driver, dsn string, readOnly bool) (*sql.DB, Dialect, error) {
	switch driver {
	case "sqlite":
		// modernc honours the mode=ro URI parameter ONLY when the filename is a
		// file: URI — a plain "path?mode=ro" is silently opened read-WRITE. Use the
		// file: form so the read-only pool is genuinely read-only (file-level, and
		// so robust across the whole pool, unlike the postgres per-connection SET).
		params := "?_journal_mode=WAL&_busy_timeout=5000"
		if readOnly {
			params = "?mode=ro&_journal_mode=WAL&_busy_timeout=5000"
		}
		db, err := sql.Open("sqlite", "file:"+dsn+params)
		if err != nil {
			return nil, Dialect{}, fmt.Errorf("open sqlite: %w", err)
		}
		return db, sqliteDialect, nil

	case "postgres":
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, Dialect{}, fmt.Errorf("open postgres: %w", err)
		}
		if readOnly {
			// Best-effort: this SET binds the read-only default to ONE pooled
			// connection, not the whole pool. Defense-in-depth only — the real
			// guarantee is that read handlers issue no writes and the sole writer
			// uses a separate pool (OpenWriteDB).
			if _, err := db.Exec("SET default_transaction_read_only = true"); err != nil {
				_ = db.Close()
				return nil, Dialect{}, fmt.Errorf("set read-only: %w", err)
			}
		}
		return db, postgresDialect, nil

	default:
		return nil, Dialect{}, fmt.Errorf("unsupported driver: %q", driver)
	}
}
