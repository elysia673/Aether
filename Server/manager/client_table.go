package manager

import (
	"Aether/tools/mux"
	"fmt"
	"io"
	"net"
	"sync"
)

type ProxyInfo struct {
	RemotePort int
	LocalPort  int
	LocalIP    string
	Protocol   string
	BindAddr   string
	Listener   io.Closer
}

type ClientTable struct {
	clientID    string
	conn        *Connection
	remoteAddr  string
	connectedAt int64
	host        string

	proxies map[int]*ProxyInfo
	proxyMu sync.RWMutex

	multiplexers map[string]*mux.Multiplexer
	pending      map[string]bool
	wsTokens     map[string]string
	tunnelTokens map[string]string // token -> key (用于隧道端口认证)
	tunnelMu     sync.RWMutex

	udpTunnels map[string]*net.UDPConn
	udpMu      sync.RWMutex
}

func NewClientTable(clientID string, conn *Connection, remoteAddr string, connectedAt int64, host string) *ClientTable {
	return &ClientTable{
		clientID:     clientID,
		conn:         conn,
		remoteAddr:   remoteAddr,
		connectedAt:  connectedAt,
		host:         host,
		proxies:      make(map[int]*ProxyInfo),
		multiplexers: make(map[string]*mux.Multiplexer),
		pending:      make(map[string]bool),
		wsTokens:     make(map[string]string),
		tunnelTokens: make(map[string]string),
		udpTunnels:   make(map[string]*net.UDPConn),
	}
}

func (t *ClientTable) ClientID() string   { return t.clientID }
func (t *ClientTable) Conn() *Connection  { return t.conn }
func (t *ClientTable) RemoteAddr() string { return t.remoteAddr }
func (t *ClientTable) ConnectedAt() int64 { return t.connectedAt }
func (t *ClientTable) Host() string       { return t.host }
func (t *ClientTable) SetHost(h string)   { t.host = h }

func (t *ClientTable) TunnelHost(publicIP string) string {
	if t.host == "" {
		return publicIP
	}
	return t.host
}

func (t *ClientTable) TunnelKey(port int) string {
	return fmt.Sprintf("%s-%d", t.clientID, port)
}

// Proxy operations

func (t *ClientTable) AddProxy(p *ProxyInfo) {
	t.proxyMu.Lock()
	t.proxies[p.RemotePort] = p
	t.proxyMu.Unlock()
}

func (t *ClientTable) RemoveProxy(port int) {
	t.proxyMu.Lock()
	delete(t.proxies, port)
	t.proxyMu.Unlock()
}

func (t *ClientTable) GetProxy(port int) *ProxyInfo {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	return t.proxies[port]
}

func (t *ClientTable) GetProxyListener(port int) io.Closer {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	if p, ok := t.proxies[port]; ok {
		return p.Listener
	}
	return nil
}

func (t *ClientTable) ListProxies() []*ProxyInfo {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	list := make([]*ProxyInfo, 0, len(t.proxies))
	for _, p := range t.proxies {
		list = append(list, p)
	}
	return list
}

func (t *ClientTable) ProxyCount() int {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	return len(t.proxies)
}

// TCP tunnel operations

func (t *ClientTable) SetPending(key string) {
	t.tunnelMu.Lock()
	t.pending[key] = true
	t.tunnelMu.Unlock()
}

func (t *ClientTable) IsPending(key string) bool {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	return t.pending[key]
}

func (t *ClientTable) PutMultiplexer(key string, mx *mux.Multiplexer) {
	t.tunnelMu.Lock()
	t.multiplexers[key] = mx
	t.tunnelMu.Unlock()
}

func (t *ClientTable) GetMultiplexer(key string) (*mux.Multiplexer, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	m, ok := t.multiplexers[key]
	if !ok {
		return nil, fmt.Errorf("no multiplexer for key %s", key)
	}
	return m, nil
}

func (t *ClientTable) RemoveTunnel(key string) {
	t.tunnelMu.Lock()
	if m := t.multiplexers[key]; m != nil {
		m.Close()
		delete(t.multiplexers, key)
	}
	delete(t.pending, key)
	t.tunnelMu.Unlock()
}

func (t *ClientTable) RemoveMux(key string) {
	t.tunnelMu.Lock()
	delete(t.multiplexers, key)
	t.tunnelMu.Unlock()
}

// WS token operations

func (t *ClientTable) StoreWSToken(token, key string) {
	t.tunnelMu.Lock()
	t.wsTokens[token] = key
	t.tunnelMu.Unlock()
}

func (t *ClientTable) GetWSToken(token string) (string, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	key, ok := t.wsTokens[token]
	if !ok {
		return "", fmt.Errorf("invalid token")
	}
	return key, nil
}

func (t *ClientTable) RemoveWSToken(token string) {
	t.tunnelMu.Lock()
	delete(t.wsTokens, token)
	t.tunnelMu.Unlock()
}

// Tunnel token operations (用于隧道端口认证)

func (t *ClientTable) StoreTunnelToken(token, key string) {
	t.tunnelMu.Lock()
	t.tunnelTokens[token] = key
	t.tunnelMu.Unlock()
}

func (t *ClientTable) GetTunnelToken(token string) (string, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	key, ok := t.tunnelTokens[token]
	if !ok {
		return "", fmt.Errorf("invalid token")
	}
	return key, nil
}

func (t *ClientTable) RemoveTunnelToken(token string) {
	t.tunnelMu.Lock()
	delete(t.tunnelTokens, token)
	t.tunnelMu.Unlock()
}

// RemoveTunnelTokenByKey 根据代理 key 删除隧道 token
func (t *ClientTable) RemoveTunnelTokenByKey(key string) {
	t.tunnelMu.Lock()
	for token, k := range t.tunnelTokens {
		if k == key {
			delete(t.tunnelTokens, token)
		}
	}
	t.tunnelMu.Unlock()
}

// UDP tunnel operations

func (t *ClientTable) SetUDPTunnel(key string, conn *net.UDPConn) {
	t.udpMu.Lock()
	if old := t.udpTunnels[key]; old != nil {
		old.Close()
	}
	t.udpTunnels[key] = conn
	t.udpMu.Unlock()
}

func (t *ClientTable) GetUDPTunnel(key string) *net.UDPConn {
	t.udpMu.RLock()
	defer t.udpMu.RUnlock()
	return t.udpTunnels[key]
}

// Cleanup closes all proxies, tunnels, and connections associated with this client

func (t *ClientTable) Cleanup() {
	t.proxyMu.Lock()
	for _, p := range t.proxies {
		if p.Listener != nil {
			p.Listener.Close()
		}
	}
	t.proxyMu.Unlock()

	t.tunnelMu.Lock()
	for _, mx := range t.multiplexers {
		mx.Close()
	}
	t.tunnelMu.Unlock()

	t.udpMu.Lock()
	for _, c := range t.udpTunnels {
		c.Close()
	}
	t.udpMu.Unlock()
}
