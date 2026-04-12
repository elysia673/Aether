package main

import (
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// GetListeningPorts 跨平台获取监听端口列表
func GetListeningPorts() ([]map[string]interface{}, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxPorts()
	case "darwin":
		return getDarwinPorts()
	default:
		return nil, fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}
}

func getLinuxPorts() ([]map[string]interface{}, error) {
	out, err := exec.Command("ss", "-tlnp").Output()
	if err != nil {
		out, err = exec.Command("netstat", "-tlnp").Output()
	}
	if err != nil {
		return nil, err
	}
	return parseNetstatOutput(string(out))
}

func getDarwinPorts() ([]map[string]interface{}, error) {
	out, err := exec.Command("lsof", "-nP", "-iTCP", "-sTCP:LISTEN").Output()
	if err != nil {
		return nil, err
	}
	return parseLsofOutput(string(out))
}

// parseNetstatOutput 解析 Linux ss/netstat 输出
func parseNetstatOutput(output string) ([]map[string]interface{}, error) {
	lines := strings.Split(output, "\n")
	var ports []map[string]interface{}

	re := regexp.MustCompile(`(?i)^(tcp|tcp6|udp|udp6)\s+LISTEN\s+\d+\s+\d+\s+([^\s]+):(\d+)\s+[^\s]+(?:\s+users:\(\(\"([^\"]+)\"|\s+(\d+)/(\S+))?`)

	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) < 4 {
			continue
		}
		protocol := strings.ToLower(matches[1])
		if strings.HasPrefix(protocol, "tcp") {
			protocol = "tcp"
		} else if strings.HasPrefix(protocol, "udp") {
			protocol = "udp"
		}
		port, _ := strconv.Atoi(matches[3])
		process := ""
		if len(matches) > 4 && matches[4] != "" {
			process = matches[4]
		} else if len(matches) > 6 && matches[6] != "" {
			process = matches[6]
		}
		ports = append(ports, map[string]interface{}{
			"protocol": protocol,
			"port":     port,
			"process":  process,
		})
	}
	return DeduplicatePorts(ports), nil
}

// parseLsofOutput 解析 macOS lsof 输出
func parseLsofOutput(output string) ([]map[string]interface{}, error) {
	lines := strings.Split(output, "\n")
	portsMap := make(map[int]map[string]interface{})

	re := regexp.MustCompile(`^(\S+)\s+\d+\s+\S+\s+\S+\s+IPv[46]\s+\S+\s+\S+\s+TCP\s+(\S+):(\d+)\s+\(LISTEN\)`)
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 4 {
			process := matches[1]
			port, _ := strconv.Atoi(matches[3])

			if existing, ok := portsMap[port]; !ok {
				portsMap[port] = map[string]interface{}{
					"protocol": "tcp",
					"port":     port,
					"process":  process,
				}
			} else if existing["process"] == "" && process != "" {
				existing["process"] = process
			}
		}
	}

	ports := make([]map[string]interface{}, 0, len(portsMap))
	for _, p := range portsMap {
		ports = append(ports, p)
	}
	return ports, nil
}
