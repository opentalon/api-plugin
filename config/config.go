package config

import (
	"encoding/json"
	"fmt"
)

// PluginConfig is the config block passed by the host via Init RPC.
// __db_driver and __db_dsn are auto-injected by the host from state.db config.
type PluginConfig struct {
	DBDriver string `json:"__db_driver"`
	DBDSN    string `json:"__db_dsn"`
	APIToken string `json:"api_token,omitempty"` // optional Bearer token for HTTP auth
}

// Parse decodes the JSON config from the host Init RPC.
func Parse(configJSON string) (*PluginConfig, error) {
	var cfg PluginConfig
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.DBDriver == "" {
		cfg.DBDriver = "sqlite"
	}
	return &cfg, nil
}
