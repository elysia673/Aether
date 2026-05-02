// Package config 提供 Aether 服务端配置管理
//
// 支持 JSON 配置文件和环境变量，配置文件优先，未设置的敏感配置回退到环境变量。
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config 是 Aether 服务端的完整配置
type Config struct {
	Server   ServerConfig `json:"server"`    // 服务器配置
	TLS      TLSConfig    `json:"tls"`       // TLS 配置
	Auth     AuthConfig   `json:"auth"`      // 认证配置
	Storage  string       `json:"storage"`   // 存储文件路径
	PublicIP string       `json:"public_ip"` // 公网 IP（留空则自动检测）
}

// ServerConfig 服务器网络配置
type ServerConfig struct {
	Addr       string `json:"addr"`        // 监听地址，如 ":9909"
	Domain     string `json:"domain"`      // 公网域名，用于返回代理地址
	TunnelPort int    `json:"tunnel_port"` // 隧道端口，如 9908
}

// TLSConfig TLS 加密配置
type TLSConfig struct {
	Enabled  bool   `json:"enabled"`   // 是否启用 TLS
	CertFile string `json:"cert_file"` // 证书文件路径
	KeyFile  string `json:"key_file"`  // 私钥文件路径
}

// AuthConfig 认证配置
type AuthConfig struct {
	APIKey      string `json:"api_key"`      // API 访问密钥
	ClientToken string `json:"client_token"` // 客户端注册令牌
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:       ":9909",
			TunnelPort: 9908,
		},
		TLS: TLSConfig{
			Enabled:  true,
			CertFile: "ssl/cert.pem",
			KeyFile:  "ssl/key.pem",
		},
		Auth:    AuthConfig{},
		Storage: "data/proxies.json",
	}
}

// Load 从指定路径加载配置文件
//
// 配置文件为 JSON 格式，未设置的敏感字段会尝试从环境变量读取：
//   - api_key: AETHER_API_KEY
//   - client_token: AETHER_CLIENT_TOKEN
//   - public_ip: AETHER_PUBLIC_IP
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// 环境变量
	if cfg.Auth.APIKey == "" {
		cfg.Auth.APIKey = os.Getenv("AETHER_API_KEY")
	}
	if cfg.Auth.ClientToken == "" {
		cfg.Auth.ClientToken = os.Getenv("AETHER_CLIENT_TOKEN")
	}
	if cfg.PublicIP == "" {
		cfg.PublicIP = os.Getenv("AETHER_PUBLIC_IP")
	}

	// 验证必填字段
	if cfg.Auth.APIKey == "" {
		return nil, fmt.Errorf("api_key is required (config file or AETHER_API_KEY env)")
	}
	if cfg.Auth.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required (config file or AETHER_CLIENT_TOKEN env)")
	}

	return cfg, nil
}
