package manager

import (
	"Aether/Server/model"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 40 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 65536
)

type Connection struct {
	wsConn      *websocket.Conn
	clientID    string
	send        chan []byte
	manager     *ClientManager
	tunnelMgr   *TunnelManager
	registered  bool
	mu          sync.RWMutex
	closeOnce   sync.Once
	connectedAt time.Time
}

func NewConnection(wsConn *websocket.Conn, mgr *ClientManager, tunnelMgr *TunnelManager) *Connection {
	return &Connection{
		wsConn:      wsConn,
		manager:     mgr,
		tunnelMgr:   tunnelMgr,
		send:        make(chan []byte, 256),
		connectedAt: time.Now(),
	}
}

// 启动读写协程
func (c *Connection) Start() {
	go c.writePump()
	go c.readPump()
}

// 读协程：处理来自 Client 的消息
func (c *Connection) readPump() {
	defer c.Close()

	c.wsConn.SetReadLimit(maxMessageSize)
	c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	c.wsConn.SetPongHandler(func(string) error {
		c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
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

// 处理收到的消息
func (c *Connection) handleMessage(msg *model.WSMessage) {
	switch msg.Type {
	case "register":
		c.handleRegister(msg.Data)
	case "response":
		// 处理命令响应，例如日志记录或转发给 HTTP 请求方
		log.Printf("client %s response: %+v", c.clientID, msg.Data)
	case "ports_list":
		var portsData model.PortsListData
		b, _ := json.Marshal(msg.Data)
		json.Unmarshal(b, &portsData)

		// 找到对应的等待通道
		if ch, ok := c.manager.GetPendingRequest(portsData.RequestID); ok {
			ch <- &portsData
			c.manager.UnregisterPendingRequest(portsData.RequestID)
		}
	case "tunnel_data":
		var data struct {
			TunnelID string `json:"tunnel_id"`
			Data     []byte `json:"data"`
		}
		b, _ := json.Marshal(msg.Data)
		json.Unmarshal(b, &data)

		tunnel, ok := c.tunnelMgr.Get(data.TunnelID) // ✅ 使用 c.tunnelMgr
		if !ok {
			return
		}
		_, err := tunnel.Conn.Write(data.Data)
		if err != nil {
			log.Printf("tunnel %s write to public conn error: %v", data.TunnelID, err)
			tunnel.Conn.Close()
			c.tunnelMgr.Remove(data.TunnelID) // ✅ 使用 c.tunnelMgr
		}
	case "pong":
		// 客户端响应心跳，仅作日志或忽略
	default:
		log.Printf("unknown message type: %s", msg.Type)
	}
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
		b, _ := json.Marshal(data)
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
		old.Close()
	}

	c.clientID = reg.ClientID
	c.registered = true
	c.manager.Add(reg.ClientID, c)

	respData, _ := json.Marshal(map[string]string{"client_id": reg.ClientID})
	resp := model.WSMessage{
		Type: "registered",
		Data: string(respData),
	}
	c.WriteJSON(&resp)

	log.Printf("client %s registered", reg.ClientID)
}

func (c *Connection) sendError(code int, message string) {
	errData, _ := json.Marshal(model.ErrorData{Code: code, Message: message})
	errMsg := model.WSMessage{
		Type: "error",
		Data: string(errData),
	}
	c.WriteJSON(&errMsg)
}

// 写协程：将 send 队列中的数据发送到 WebSocket
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

// 写 JSON 消息到发送队列
func (c *Connection) WriteJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
	default:
		// 发送队列满，关闭连接
		return c.Close()
	}
	return nil
}

// 关闭连接并从管理器移除
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		if c.clientID != "" {
			c.manager.Remove(c.clientID)
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
