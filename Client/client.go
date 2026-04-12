package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	maxMessageSize = 65536 // 1MB
)

// Client 长连接客户端
type Client struct {
	conn       *websocket.Conn
	url        string
	id         string
	token      string
	stopCh     chan struct{}
	msgHandler *MessageHandler
	tunnels    sync.Map // key: tunnelID, value: net.Conn/
}

// NewClient 创建客户端实例
func NewClient(url, id, token string) *Client {
	c := &Client{
		url:    url,
		id:     id,
		token:  token,
		stopCh: make(chan struct{}),
	}
	c.msgHandler = NewMessageHandler(c)
	return c
}

// Run 启动客户端，包含自动重连
func (c *Client) Run() {
	for {
		select {
		case <-c.stopCh:
			log.Println("client stopped")
			return
		default:
			if err := c.connectAndServe(); err != nil {
				log.Printf("connection error: %v, reconnecting in %v...", err, reconnectDelay)
				time.Sleep(reconnectDelay)
			}
		}
	}
}

// Stop 停止客户端
func (c *Client) Stop() {
	close(c.stopCh)
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) connectAndServe() error {
	log.Printf("connecting to %s", c.url)
	conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return err
	}
	conn.SetReadLimit(maxMessageSize)
	c.conn = conn
	defer conn.Close()

	if err := c.register(); err != nil {
		return err
	}

	return c.messageLoop()
}

func (c *Client) register() error {
	regMsg := map[string]interface{}{
		"type": "register",
		"data": map[string]string{
			"client_id": c.id,
			"token":     c.token,
		},
	}
	if err := c.conn.WriteJSON(regMsg); err != nil {
		return fmt.Errorf("write register: %w", err)
	}

	var resp map[string]interface{}
	if err := c.conn.ReadJSON(&resp); err != nil {
		return fmt.Errorf("read register response: %w", err)
	}

	if resp["type"] != "registered" {
		return fmt.Errorf("registration failed: %v", resp)
	}
	log.Printf("registered successfully: %+v", resp)
	return nil
}

func (c *Client) messageLoop() error {
	for {
		var msg map[string]interface{}
		if err := c.conn.ReadJSON(&msg); err != nil {
			return err
		}
		log.Printf("received: %+v", msg)

		// 异步处理，避免阻塞读循环
		go c.msgHandler.Handle(msg)
	}
}

// WriteJSON 向连接写入 JSON 消息
func (c *Client) WriteJSON(v interface{}) error {
	if c.conn == nil {
		return fmt.Errorf("connection not established")
	}
	return c.conn.WriteJSON(v)
}
