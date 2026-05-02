package config

import (
	"fmt"
	"os"
)

type ClientConfig struct {
	ServerURL             string `json:"server_url"`
	ClientToken           string `json:"client_token"`
	ClientID              string `json:"client_id"`
	UseHTTP               bool   `json:"use_http"`
	TLSSNI                string `json:"tls_sni"`
	Origin                string `json:"origin"`
	ReconnectDelaySeconds int    `json:"reconnect_delay_seconds"`
}

func defaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientID:              "raspberry-pi-01",
		ReconnectDelaySeconds: 5,
	}
}

func LoadClient(path string) (*ClientConfig, error) {
	cfg := defaultClientConfig()

	if path != "" {
		if err := loadJSON(path, cfg); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	cfg.ServerURL = envStr("AETHER_WS_URL", cfg.ServerURL)
	cfg.ClientToken = envStr("AETHER_CLIENT_TOKEN", cfg.ClientToken)
	cfg.ClientID = envStr("AETHER_CLIENT_ID", cfg.ClientID)
	cfg.UseHTTP = envBool("AETHER_USE_HTTP", cfg.UseHTTP)
	cfg.TLSSNI = envStr("AETHER_TLS_SNI", cfg.TLSSNI)
	cfg.Origin = envStr("AETHER_ORIGIN", cfg.Origin)
	cfg.ReconnectDelaySeconds = envInt("AETHER_RECONNECT_DELAY", cfg.ReconnectDelaySeconds)

	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("server_url is required (set in config file or AETHER_WS_URL env)")
	}
	if cfg.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required (set in config file or AETHER_CLIENT_TOKEN env)")
	}

	return cfg, nil
}
