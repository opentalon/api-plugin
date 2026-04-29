package config

import "testing"

func TestParse(t *testing.T) {
	cfg, err := Parse(`{"__db_driver":"postgres","__db_dsn":"postgres://localhost/test","api_token":"secret"}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DBDriver != "postgres" {
		t.Errorf("DBDriver = %q, want postgres", cfg.DBDriver)
	}
	if cfg.DBDSN != "postgres://localhost/test" {
		t.Errorf("DBDSN = %q", cfg.DBDSN)
	}
	if cfg.APIToken != "secret" {
		t.Errorf("APIToken = %q, want secret", cfg.APIToken)
	}
}

func TestParse_DefaultDriver(t *testing.T) {
	cfg, err := Parse(`{"__db_dsn":"/tmp/state.db"}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DBDriver != "sqlite" {
		t.Errorf("DBDriver = %q, want sqlite", cfg.DBDriver)
	}
}

func TestParse_Invalid(t *testing.T) {
	_, err := Parse(`not json`)
	if err == nil {
		t.Fatal("expected error")
	}
}
