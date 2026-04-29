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

// OpenDB opens a read-only database connection.
func OpenDB(driver, dsn string) (*sql.DB, Dialect, error) {
	switch driver {
	case "sqlite":
		db, err := sql.Open("sqlite", dsn+"?mode=ro&_journal_mode=WAL&_busy_timeout=5000")
		if err != nil {
			return nil, Dialect{}, fmt.Errorf("open sqlite: %w", err)
		}
		return db, sqliteDialect, nil

	case "postgres":
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, Dialect{}, fmt.Errorf("open postgres: %w", err)
		}
		if _, err := db.Exec("SET default_transaction_read_only = true"); err != nil {
			_ = db.Close()
			return nil, Dialect{}, fmt.Errorf("set read-only: %w", err)
		}
		return db, postgresDialect, nil

	default:
		return nil, Dialect{}, fmt.Errorf("unsupported driver: %q", driver)
	}
}
