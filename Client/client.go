package main

import (
	"Aether/Client/conn"
	"Aether/Client/handler"
	"Aether/pkg/model"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Client is an Aether client that maintains a WebSocket connection to the server,
// handles registration, and delegates tunnel management to the handler package.
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

// NewClient creates a new Client instance.
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

// Run starts the main client loop with automatic reconnection.
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

// Stop signals the client to shut down.
func (c *Client) Stop() {
	close(c.stopCh)
}

// connectAndServe dials the server, registers the client, then runs
// the message pumps until the connection terminates.
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

	// Register before starting the message pumps.
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

// registerRaw performs the registration handshake before the Connection pumps start.
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
