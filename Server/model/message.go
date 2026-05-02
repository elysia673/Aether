package model

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

type RegisterData struct {
	ClientID string `json:"client_id"`
	Token    string `json:"token"`
}

type CommandData struct {
	RequestID  string `json:"request_id"`
	RemotePort int    `json:"remote_port,omitempty"`
	LocalPort  int    `json:"local_port,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	BindAddr   string `json:"bind_addr,omitempty"`
	Command    string `json:"command,omitempty"`
	ServerHost string `json:"server_host,omitempty"`
	TunnelPort int    `json:"tunnel_port,omitempty"` // 隧道端口
	Token      string `json:"token,omitempty"`
	LocalIP    string `json:"local_ip,omitempty"`
}

type ErrorData struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// 下行命令：请求端口列表
type ListPortsCmd struct {
	RequestID string `json:"request_id"`
}

// 单个端口信息
type PortInfo struct {
	Protocol string `json:"protocol"` // "tcp" 或 "udp"
	Port     int    `json:"port"`
	Process  string `json:"process,omitempty"` // 可选，进程名
}

// 上行响应：端口列表数据
type PortsListData struct {
	RequestID string     `json:"request_id"`
	Ports     []PortInfo `json:"ports"`
	Error     string     `json:"error,omitempty"`
}
