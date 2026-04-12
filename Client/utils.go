package main

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
