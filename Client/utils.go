package main

import (
	"net/url"
	"os"
)

// tlsServerName 从 serverURL 解析 TLS SNI 主机名
// 可通过 AETHER_TLS_SNI 环境变量覆盖
func tlsServerName(serverURL string) string {
	if host := os.Getenv("AETHER_TLS_SNI"); host != "" {
		return host
	}
	u, err := url.Parse(serverURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// originHeader 从 serverURL 构造 Origin 请求头
// 可通过 AETHER_ORIGIN 环境变量覆盖
func originHeader(serverURL string) string {
	if origin := os.Getenv("AETHER_ORIGIN"); origin != "" {
		return origin
	}
	host := tlsServerName(serverURL)
	if host == "" {
		return ""
	}
	if useHTTP {
		return "http://" + host
	}
	return "https://" + host
}

// DeduplicatePorts 按端口号去重
func DeduplicatePorts(ports []map[string]interface{}) []map[string]interface{} {
	seen := make(map[int]bool)
	result := make([]map[string]interface{}, 0, len(ports))
	for _, p := range ports {
		port, ok := p["port"].(int)
		if !ok {
			continue
		}
		if !seen[port] {
			seen[port] = true
			result = append(result, p)
		}
	}
	return result
}
