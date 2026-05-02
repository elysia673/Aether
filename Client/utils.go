package main

import (
	"Aether/pkg/model"
	"net/url"
)

func tlsServerName(serverURL, override string) string {
	if override != "" {
		return override
	}
	u, err := url.Parse(serverURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func originHeader(serverURL string, useHTTP bool, override string) string {
	if override != "" {
		return override
	}
	host := tlsServerName(serverURL, "")
	if host == "" {
		return ""
	}
	if useHTTP {
		return "http://" + host
	}
	return "https://" + host
}

func DeduplicatePorts(ports []model.PortInfo) []model.PortInfo {
	seen := make(map[int]bool)
	result := make([]model.PortInfo, 0, len(ports))
	for _, p := range ports {
		if !seen[p.Port] {
			seen[p.Port] = true
			result = append(result, p)
		}
	}
	return result
}
