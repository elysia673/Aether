package manager

import (
	"Aether/Server/model"
	"errors"
	"sync"
)

type Config struct {
	ClientToken string // client 连接时使用的 Token
}

type ClientManager struct {
	clients         sync.Map // key: clientID, value: *Connection
	pendingRequests sync.Map // key: requestID, value: chan *model.PortsListData
	config          Config
}

func NewClientManager(cfg Config) *ClientManager {
	return &ClientManager{
		config: cfg,
	}
}

func (m *ClientManager) Add(clientID string, conn *Connection) {
	m.clients.Store(clientID, conn)
}

func (m *ClientManager) Remove(clientID string) {
	m.clients.Delete(clientID)
}

func (m *ClientManager) Get(clientID string) (*Connection, bool) {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return nil, false
	}
	return val.(*Connection), true
}

func (m *ClientManager) ListClients() []ClientInfo {
	var list []ClientInfo
	m.clients.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		list = append(list, conn.GetInfo())
		return true
	})
	return list
}

// 向指定客户端发送命令（用于 API Handler 调用）
func (m *ClientManager) SendCommand(clientID string, cmd interface{}) error {
	conn, ok := m.Get(clientID)
	if !ok {
		return ErrClientNotFound
	}
	// 将 cmd 包装成 WSMessage，添加 request_id 以便追踪
	// 这里假设 cmd 已经是完整的消息对象，或我们在此构造
	return conn.WriteJSON(cmd)
}

// 错误定义
var ErrClientNotFound = errors.New("client not found")

type ClientInfo struct {
	ID          string `json:"id"`
	RemoteAddr  string `json:"remote_addr"`
	ConnectedAt int64  `json:"connected_at"`
}

func (m *ClientManager) RegisterPendingRequest(requestID string, ch chan *model.PortsListData) {
	m.pendingRequests.Store(requestID, ch)
}

func (m *ClientManager) UnregisterPendingRequest(requestID string) {
	m.pendingRequests.Delete(requestID)
}

func (m *ClientManager) GetPendingRequest(requestID string) (chan *model.PortsListData, bool) {
	val, ok := m.pendingRequests.Load(requestID)
	if !ok {
		return nil, false
	}
	return val.(chan *model.PortsListData), true
}
