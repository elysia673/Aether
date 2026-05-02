package model

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

type RegisterData struct {
	ClientID string `json:"client_id"`
	Token    string `json:"token"`
}

type RegisteredData struct {
	ClientID   string `json:"client_id"`
	ServerHost string `json:"server_host"`
}

type CommandData struct {
	RequestID  string `json:"request_id"`
	RemotePort int    `json:"remote_port,omitempty"`
	LocalPort  int    `json:"local_port,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	BindAddr   string `json:"bind_addr,omitempty"`
	Command    string `json:"command,omitempty"`
	ServerHost string `json:"server_host,omitempty"`
	TunnelPort int    `json:"tunnel_port,omitempty"`
	Token      string `json:"token,omitempty"`
	LocalIP    string `json:"local_ip,omitempty"`
}

type ErrorData struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ProxyClosedData struct {
	Key string `json:"key"`
}

type ListPortsCmd struct {
	RequestID string `json:"request_id"`
}

type PortInfo struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
	Process  string `json:"process,omitempty"`
}

type PortsListData struct {
	RequestID string     `json:"request_id"`
	Ports     []PortInfo `json:"ports"`
	Error     string     `json:"error,omitempty"`
}

type TunnelAuthMsg struct {
	Type string         `json:"type"`
	Data TunnelAuthData `json:"data"`
}

type TunnelAuthData struct {
	Token string `json:"token"`
}

type TunnelReadyMsg struct {
	Type string          `json:"type"`
	Data TunnelReadyData `json:"data"`
}

type TunnelReadyData struct {
	Status string `json:"status"`
}
