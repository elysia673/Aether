package main

import (
	"Aether/Aether_Client/conn"
	"Aether/Aether_Client/handler"
	"Aether/common/model"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Client 是 Aether 客户端，维护与服务器的 WebSocket 连接，
// 处理注册并将隧道管理委托给 handler 包。
type Client struct {
	url            string
	id             string
	token          string
	useHTTP        bool
	tlsSNI         string
	origin         string
	reconnectDelay time.Duration
	stopCh         chan struct{}
}

// NewClient 创建新的客户端实例。
func NewClient(url, id, token string, useHTTP bool, tlsSNI, origin string, reconnectDelay time.Duration) *Client {
	return &Client{
		url:            url,
		id:             id,
		token:          token,
		useHTTP:        useHTTP,
		tlsSNI:         tlsSNI,
		origin:         origin,
		reconnectDelay: reconnectDelay,
		stopCh:         make(chan struct{}),
	}
}

// Run 启动客户端主循环，支持自动重连。
func (c *Client) Run() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if err := c.connectAndServe(); err != nil {
			log.Printf("connection error: %v", err)
		}

		select {
		case <-c.stopCh:
			return
		case <-time.After(c.reconnectDelay):
		}
	}
}

// Stop 通知客户端关闭。
func (c *Client) Stop() {
	close(c.stopCh)
}

// connectAndServe 连接服务器，注册客户端，然后运行消息泵直到连接终止。
func (c *Client) connectAndServe() error {
	log.Printf("connecting to %s", c.url)

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !c.useHTTP {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if sni := tlsServerName(c.url, c.tlsSNI); sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !c.useHTTP {
		if origin := originHeader(c.url, c.useHTTP, c.origin); origin != "" {
			header.Set("Origin", origin)
		}
	}

	wsConn, _, err := dialer.Dial(c.url, header)
	if err != nil {
		return err
	}

	// 在启动消息泵之前进行注册。
	if err := c.registerRaw(wsConn); err != nil {
		wsConn.Close()
		return err
	}

	h := handler.New(handler.Config{
		ClientID:       c.id,
		UseHTTP:        c.useHTTP,
		SNIOverride:    tlsServerName(c.url, c.tlsSNI),
		OriginOverride: originHeader(c.url, c.useHTTP, c.origin),
	})

	connection := conn.New(wsConn, h.Handle)
	h.SetSender(connection)
	connection.Start()
	defer func() {
		h.Stop()
	}()

	select {
	case <-connection.Done():
	case <-c.stopCh:
		connection.Close()
	}

	return nil
}

// registerRaw 在消息泵启动之前执行注册握手。
func (c *Client) registerRaw(wsConn *websocket.Conn) error {
	regMsg := model.WSMessage{
		Type: "register",
		Data: model.RegisterData{
			ClientID: c.id,
			Token:    c.token,
		},
	}
	if err := wsConn.WriteJSON(&regMsg); err != nil {
		return fmt.Errorf("write register: %w", err)
	}

	var resp model.WSMessage
	if err := wsConn.ReadJSON(&resp); err != nil {
		return fmt.Errorf("read register response: %w", err)
	}

	if resp.Type != "registered" {
		return fmt.Errorf("registration failed: %v", resp)
	}

	var regData model.RegisteredData
	if dataStr, ok := resp.Data.(string); ok {
		if err := json.Unmarshal([]byte(dataStr), &regData); err != nil {
			return fmt.Errorf("unmarshal registered data: %w", err)
		}
	}
	log.Printf("registered: client_id=%s, server_host=%s", regData.ClientID, regData.ServerHost)
	return nil
}
