package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// 最大消息大小限制
const (
	maxMessageSize = 65536
)

// Client Aether 客户端
//
// 负责与服务端建立 WebSocket 连接、注册身份、接收代理命令。
// 支持自动重连和优雅关闭。
type Client struct {
	conn       *websocket.Conn
	connMu     sync.Mutex      // 连接锁
	url        string          // 服务端 WebSocket URL
	id         string          // 客户端 ID
	token      string          // 注册令牌
	stopCh     chan struct{}   // 停止信号
	msgHandler *MessageHandler // 消息处理器
}

// NewClient 创建新的客户端实例
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

// Run 启动客户端主循环
//
// 自动重连机制：连接断开后等待 reconnectDelay 再重试。
// 收到 stopCh 信号时退出。
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

func (c *Client) Stop() {
	close(c.stopCh)
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
}

func (c *Client) connectAndServe() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.msgHandler.SetContext(ctx)

	log.Printf("connecting to %s", c.url)

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !useHTTP {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if sni := tlsServerName(c.url); sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !useHTTP {
		if origin := originHeader(c.url); origin != "" {
			header.Set("Origin", origin)
		}
	}

	conn, _, err := dialer.Dial(c.url, header)
	if err != nil {
		return err
	}
	conn.SetReadLimit(maxMessageSize)
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	defer func() {
		c.connMu.Lock()
		c.conn = nil
		c.connMu.Unlock()
		conn.Close()
	}()

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

		go c.msgHandler.Handle(msg)
	}
}

func (c *Client) WriteJSON(v interface{}) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("connection not established")
	}
	return conn.WriteJSON(v)
}
