// Package manager 提供客户端连接和代理管理
//
// 负责维护客户端连接状态、代理映射和隧道多路复用器。
package manager

import (
	"Aether/pkg/model"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

// Config 客户端管理器配置
type Config struct {
	ClientToken   string                                  // 客户端注册令牌
	PublicIP      string                                  // 服务器公网 IP
	OnClientReady func(clientID string, conn *Connection) // 客户端注册完成回调
}

// ClientManager 管理所有客户端连接
//
// 使用 sync.Map 实现并发安全的客户端存储，
// portIndex 提供端口到客户端的快速查找。
type ClientManager struct {
	clients         sync.Map       // clientID -> *ClientTable
	pendingRequests sync.Map       // requestID -> chan *PortsListData
	config          Config         // 管理器配置
	portIndex       map[int]string // port -> clientID 索引
	portIdxMu       sync.RWMutex   // 端口索引锁
}

// NewClientManager 创建客户端管理器
func NewClientManager(cfg Config) *ClientManager {
	return &ClientManager{
		config:    cfg,
		portIndex: make(map[int]string),
	}
}

// OnClientReady 触发客户端注册完成回调
func (m *ClientManager) OnClientReady(clientID string, conn *Connection) {
	if m.config.OnClientReady != nil {
		m.config.OnClientReady(clientID, conn)
	}
}

// SetOnClientReady 设置客户端注册完成回调
func (m *ClientManager) SetOnClientReady(callback func(string, *Connection)) {
	m.config.OnClientReady = callback
}

// Add 注册客户端
func (m *ClientManager) Add(clientID string, table *ClientTable) {
	m.clients.Store(clientID, table)
}

func (m *ClientManager) Remove(clientID string) {
	table, ok := m.Get(clientID)
	if !ok {
		return
	}
	table.Cleanup()
	m.clients.Delete(clientID)

	m.portIdxMu.Lock()
	for port, cid := range m.portIndex {
		if cid == clientID {
			delete(m.portIndex, port)
		}
	}
	m.portIdxMu.Unlock()
}

func (m *ClientManager) Get(clientID string) (*ClientTable, bool) {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return nil, false
	}
	table, ok := val.(*ClientTable)
	if !ok {
		return nil, false
	}
	return table, true
}

func (m *ClientManager) ListClients() []ClientInfo {
	var list []ClientInfo
	m.clients.Range(func(key, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		list = append(list, ClientInfo{
			ID:          table.ClientID(),
			RemoteAddr:  table.RemoteAddr(),
			ConnectedAt: table.ConnectedAt(),
			ProxyCount:  table.ProxyCount(),
			Host:        table.Host(),
		})
		return true
	})
	return list
}

func (m *ClientManager) SendCommand(clientID string, cmd interface{}) error {
	table, ok := m.Get(clientID)
	if !ok {
		return ErrClientNotFound
	}
	return table.Conn().WriteJSON(cmd)
}

var ErrClientNotFound = errors.New("client not found")

type ClientInfo struct {
	ID          string `json:"id"`
	RemoteAddr  string `json:"remote_addr"`
	ConnectedAt int64  `json:"connected_at"`
	ProxyCount  int    `json:"proxy_count"`
	Host        string `json:"host"`
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
	ch, ok := val.(chan *model.PortsListData)
	if !ok {
		return nil, false
	}
	return ch, true
}

func (m *ClientManager) GetPublicIP() string {
	return m.config.PublicIP
}

// Port index for fast proxy lookup

func (m *ClientManager) RegisterPort(clientID string, port int) {
	m.portIdxMu.Lock()
	m.portIndex[port] = clientID
	m.portIdxMu.Unlock()
}

func (m *ClientManager) UnregisterPort(port int) {
	m.portIdxMu.Lock()
	delete(m.portIndex, port)
	m.portIdxMu.Unlock()
}

func (m *ClientManager) GetClientIDByPort(port int) (string, bool) {
	m.portIdxMu.RLock()
	defer m.portIdxMu.RUnlock()
	clientID, ok := m.portIndex[port]
	return clientID, ok
}

// FindTableByWSToken looks up the ClientTable that owns a given WS tunnel token
func (m *ClientManager) FindTableByWSToken(token string) (*ClientTable, string, error) {
	var found *ClientTable
	var foundKey string
	m.clients.Range(func(_, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		key, err := table.GetWSToken(token)
		if err == nil {
			found = table
			foundKey = key
			return false
		}
		return true
	})
	if found == nil {
		return nil, "", fmt.Errorf("invalid token")
	}
	return found, foundKey, nil
}

// ListAllProxies returns all proxy info across all clients
func (m *ClientManager) ListAllProxies() []map[string]interface{} {
	publicIP := m.config.PublicIP
	var result []map[string]interface{}
	m.clients.Range(func(_, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		for _, p := range table.ListProxies() {
			var portStr string
			switch v := p.Listener.(type) {
			case net.Listener:
				_, portStr, _ = net.SplitHostPort(v.Addr().String())
			}
			if portStr == "" {
				portStr = strconv.Itoa(p.RemotePort)
			}
			result = append(result, map[string]interface{}{
				"remote_port": p.RemotePort,
				"local_port":  p.LocalPort,
				"public_addr": publicIP + ":" + portStr,
				"client_id":   table.ClientID(),
			})
		}
		return true
	})
	return result
}

// RangeClients 遍历所有客户端
func (m *ClientManager) RangeClients(fn func(clientID string, table *ClientTable) bool) {
	m.clients.Range(func(key, value interface{}) bool {
		clientID, ok := key.(string)
		if !ok {
			return true
		}
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		return fn(clientID, table)
	})
}
