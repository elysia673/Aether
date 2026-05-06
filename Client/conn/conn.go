// Package conn provides a WebSocket connection wrapper with heartbeat detection
// and concurrent-safe message passing via a send channel.
//
// Follows the server's manager/connection.go readPump/writePump pattern.
package conn

import (
	"Aether/pkg/model"
	"encoding/json"
	"fmt"
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

// MessageHandler is called for each received WSMessage.
type MessageHandler func(msg *model.WSMessage)

// Connection wraps a WebSocket connection with heartbeat detection
// and concurrent-safe writes via a buffered send channel.
type Connection struct {
	wsConn    *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
	onMessage MessageHandler
}

// New creates a new Connection wrapping the given WebSocket connection.
func New(wsConn *websocket.Conn, onMessage MessageHandler) *Connection {
	return &Connection{
		wsConn:    wsConn,
		send:      make(chan []byte, 256),
		done:      make(chan struct{}),
		onMessage: onMessage,
	}
}

// Start launches the read and write pump goroutines.
func (c *Connection) Start() {
	go c.readPump()
	go c.writePump()
}

// Done returns a channel that closes when the connection terminates.
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// readPump reads messages from the WebSocket and dispatches them to the handler.
// It manages read deadlines and pong heartbeat responses.
func (c *Connection) readPump() {
	defer c.Close()

	c.wsConn.SetReadLimit(maxMessageSize)
	_ = c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	c.wsConn.SetPongHandler(func(string) error {
		return c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, msg, err := c.wsConn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("conn: read error: %v", err)
			}
			break
		}

		var wsMsg model.WSMessage
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			log.Printf("conn: unmarshal error: %v", err)
			continue
		}

		if c.onMessage != nil {
			c.onMessage(&wsMsg)
		}
	}
}

// writePump sends queued messages and periodic pings to keep the connection alive.
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.wsConn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// WriteJSON marshals v to JSON and queues it for sending through the write pump.
func (c *Connection) WriteJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
		return nil
	case <-c.done:
		return fmt.Errorf("connection closed")
	}
}

// Close terminates the connection safely, ensuring cleanup happens exactly once.
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		close(c.send)
		c.wsConn.Close()
	})
}
