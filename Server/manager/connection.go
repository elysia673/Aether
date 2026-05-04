package manager

import (
	"Aether/pkg/model"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket 心跳和消息大小常量
const (
	writeWait      = 10 * time.Second // 写超时
	pongWait       = 40 * time.Second // 等待 pong 超时
	pingPeriod     = 30 * time.Second // 发送 ping 间隔
	maxMessageSize = 65536            // 最大消息大小
)

// Connection 封装 WebSocket 连接
//
// 提供并发安全的读写、心跳检测和自动重连支持。
// 使用 writePump/readPump 模式处理消息收发。
type Connection struct {
	wsConn      *websocket.Conn
	clientID    string
	table       *ClientTable
	host        string
	remoteIP    string
	send        chan []byte   // 发送缓冲区
	done        chan struct{} // 关闭信号
	manager     *ClientManager
	registered  bool // 是否已注册
	mu          sync.RWMutex
	closeOnce   sync.Once
	connectedAt time.Time
}

// NewConnection 创建新的 WebSocket 连接封装
func NewConnection(wsConn *websocket.Conn, mgr *ClientManager) *Connection {
	return &Connection{
		wsConn:      wsConn,
		manager:     mgr,
		send:        make(chan []byte, 256),
		done:        make(chan struct{}),
		connectedAt: time.Now(),
	}
}

// Start 启动读写协程和注册超时检测
func (c *Connection) Start() {
	go c.writePump()
	go c.readPump()

	go func() {
		time.Sleep(30 * time.Second)
		if !c.IsRegistered() {
			log.Println("client registration timeout, closing connection")
			c.Close()
		}
	}()
}

// readPump 读取 WebSocket 消息
//
// 负责：
// - 设置读限制和 pong 处理器
// - 读取并解析消息
// - 分发到对应处理器
func (c *Connection) readPump() {
	defer c.Close()

	c.wsConn.SetReadLimit(maxMessageSize)
	if err := c.wsConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		log.Printf("set initial read deadline error: %v", err)
	}
	c.wsConn.SetPongHandler(func(string) error {
		return c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, msg, err := c.wsConn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ws read error: %v", err)
			}
			break
		}

		var wsMsg model.WSMessage
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			log.Printf("json unmarshal error: %v", err)
			continue
		}

		c.handleMessage(&wsMsg)
	}
}

func (c *Connection) handleMessage(msg *model.WSMessage) {
	switch msg.Type {
	case "register":
		c.handleRegister(msg.Data)
	case "response":
		log.Printf("client %s response: %+v", c.clientID, msg.Data)
	case "ports_list":
		var portsData model.PortsListData
		b, err := json.Marshal(msg.Data)
		if err != nil {
			log.Printf("ports_list marshal error: %v", err)
			return
		}
		if err := json.Unmarshal(b, &portsData); err != nil {
			log.Printf("ports_list unmarshal error: %v", err)
			return
		}

		if ch, ok := c.manager.GetPendingRequest(portsData.RequestID); ok {
			ch <- &portsData
			c.manager.UnregisterPendingRequest(portsData.RequestID)
		}
	case "p2p_established", "p2p_closed":
		c.manager.DispatchP2PMessage(c.clientID, msg)
	case "pong":
	default:
		log.Printf("unknown message type: %s", msg.Type)
	}
}

func (c *Connection) getRemoteIP() string {
	addr := c.wsConn.RemoteAddr()
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return tcpAddr.IP.String()
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

func (c *Connection) handleRegister(data interface{}) {
	var reg model.RegisterData

	switch v := data.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &reg); err != nil {
			c.sendError(400, "invalid register data")
			return
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			c.sendError(400, "invalid register data")
			return
		}
		if err := json.Unmarshal(b, &reg); err != nil {
			c.sendError(400, "invalid register data")
			return
		}
	}

	if reg.Token != c.manager.config.ClientToken {
		c.sendError(401, "invalid token")
		c.Close()
		return
	}

	if reg.ClientID == "" {
		c.sendError(400, "client_id required")
		return
	}

	if old, exists := c.manager.Get(reg.ClientID); exists {
		old.Conn().Close()
	}

	c.mu.Lock()
	c.clientID = reg.ClientID
	c.registered = true
	c.remoteIP = c.getRemoteIP()
	c.mu.Unlock()

	table := NewClientTable(reg.ClientID, c, c.remoteIP, c.connectedAt.Unix(), c.host)
	c.table = table
	c.manager.Add(reg.ClientID, table)

	serverHost := c.GetHost()
	if serverHost == "" {
		serverHost = c.remoteIP
	}
	respData, err := json.Marshal(map[string]string{
		"client_id":   reg.ClientID,
		"server_host": serverHost,
	})
	if err != nil {
		log.Printf("marshal registered response error: %v", err)
		return
	}
	resp := model.WSMessage{
		Type: "registered",
		Data: string(respData),
	}
	if err := c.WriteJSON(&resp); err != nil {
		log.Printf("write registered response error: %v", err)
		return
	}

	log.Printf("client %s registered, server_host=%s", reg.ClientID, serverHost)

	// 触发客户端注册完成回调
	c.manager.OnClientReady(reg.ClientID, c)
}

func (c *Connection) sendError(code int, message string) {
	errData, err := json.Marshal(model.ErrorData{Code: code, Message: message})
	if err != nil {
		log.Printf("marshal error response error: %v", err)
		return
	}
	errMsg := model.WSMessage{
		Type: "error",
		Data: string(errData),
	}
	if err := c.WriteJSON(&errMsg); err != nil {
		log.Printf("write error response error: %v", err)
	}
}

// writePump 写入 WebSocket 消息
//
// 负责：
// - 从 send 通道取消息并发送
// - 定期发送 ping 保持连接
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.wsConn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Connection) WriteJSON(v interface{}) (err error) {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
	case <-c.done:
		return fmt.Errorf("connection closed")
	}
	return nil
}

func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		clientID := c.clientID
		c.registered = false
		c.mu.Unlock()

		if clientID != "" {
			c.manager.Remove(clientID)
		}
		close(c.send)
		c.wsConn.Close()
	})
	return nil
}

func (c *Connection) IsRegistered() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.registered
}

func (c *Connection) GetInfo() ClientInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return ClientInfo{
		ID:          c.clientID,
		RemoteAddr:  c.wsConn.RemoteAddr().String(),
		ConnectedAt: c.connectedAt.Unix(),
	}
}

func (c *Connection) GetRemoteAddr() string {
	return c.wsConn.RemoteAddr().String()
}

func (c *Connection) SetHost(host string) {
	c.mu.Lock()
	c.host = host
	c.mu.Unlock()
}

func (c *Connection) GetHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.host
}

func (c *Connection) GetTunnelHost() string {
	host := c.GetHost()
	if host == "" {
		host = c.manager.GetPublicIP()
	}
	return host
}

func (c *Connection) GetRemoteIP() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.remoteIP
}

func (c *Connection) Table() *ClientTable {
	return c.table
}
