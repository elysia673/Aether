package main

import (
	"Aether/tools/mux"
	"Aether/tools/proto"
	"Aether/tools/wsconn"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageHandler 处理从服务端收到的消息
//
// 管理隧道生命周期，支持按 key 停止特定隧道。
type MessageHandler struct {
	client     *Client
	ctx        context.Context
	tunnelMu   sync.Mutex                    // 隧道锁
	tunnelCtxs map[string]context.CancelFunc // key -> 取消函数
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(c *Client) *MessageHandler {
	return &MessageHandler{
		client:     c,
		ctx:        context.Background(),
		tunnelCtxs: make(map[string]context.CancelFunc),
	}
}

// SetContext 设置父上下文（连接断开时取消所有隧道）
func (h *MessageHandler) SetContext(ctx context.Context) {
	h.ctx = ctx
}

// RegisterTunnel 注册隧道取消函数
func (h *MessageHandler) RegisterTunnel(key string, cancel context.CancelFunc) {
	h.tunnelMu.Lock()
	h.tunnelCtxs[key] = cancel
	h.tunnelMu.Unlock()
}

// StopTunnel 停止指定隧道
func (h *MessageHandler) StopTunnel(key string) {
	h.tunnelMu.Lock()
	if cancel, ok := h.tunnelCtxs[key]; ok {
		cancel()
		delete(h.tunnelCtxs, key)
	}
	h.tunnelMu.Unlock()
}

// Handle 分发处理消息
func (h *MessageHandler) Handle(msg map[string]interface{}) {
	msgType, _ := msg["type"].(string)
	switch msgType {
	case "proxy":
		h.handleProxy(msg)
	case "proxy_closed":
		h.handleProxyClosed(msg)
	}
}

func parseData(msg map[string]interface{}) map[string]interface{} {
	if data, ok := msg["data"].(map[string]interface{}); ok {
		return data
	}
	if str, ok := msg["data"].(string); ok {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(str), &m); err != nil {
			log.Printf("parseData unmarshal error: %v", err)
			return nil
		}
		return m
	}
	return nil
}

func (h *MessageHandler) handleProxyClosed(msg map[string]interface{}) {
	data := parseData(msg)
	if data == nil {
		return
	}
	key, _ := data["key"].(string)
	if key == "" {
		return
	}
	log.Printf("proxy closed by server: %s, stopping tunnel", key)
	h.StopTunnel(key)
}

func (h *MessageHandler) handleProxy(msg map[string]interface{}) {
	data := parseData(msg)
	if data == nil {
		log.Printf("proxy message data is nil")
		return
	}

	serverHost, _ := data["server_host"].(string)
	remotePort, ok := data["remote_port"].(float64)
	if !ok {
		log.Printf("invalid remote_port in proxy message")
		return
	}
	token, _ := data["token"].(string)
	localPort, ok := data["local_port"].(float64)
	if !ok {
		log.Printf("invalid local_port in proxy message")
		return
	}
	localIP, _ := data["local_ip"].(string)
	if localIP == "" {
		localIP = "127.0.0.1"
	}

	// 隧道端口（独立于代理端口）
	tunnelPort := 0
	if tp, ok := data["tunnel_port"].(float64); ok && tp > 0 {
		tunnelPort = int(tp)
	}

	log.Printf("proxy command: serverHost=%s, remotePort=%d, localPort=%d, localIP=%s, token=%s, protocol=%s, tunnelPort=%d",
		serverHost, int(remotePort), int(localPort), localIP, token, data["protocol"], tunnelPort)

	protocol, _ := data["protocol"].(string)
	key := fmt.Sprintf("%s-%d", h.client.id, int(remotePort))

	if protocol == "websocket" {
		tunnelCtx, tunnelCancel := context.WithCancel(h.ctx)
		h.RegisterTunnel(key, tunnelCancel)
		go h.runTunnelWS(tunnelCtx, serverHost, token, int(localPort), localIP, key)
		return
	}

	tunnelCtx, tunnelCancel := context.WithCancel(h.ctx)
	h.RegisterTunnel(key, tunnelCancel)
	const tunnelPoolSize = 5
	for i := 0; i < tunnelPoolSize; i++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC in tunnel goroutine %d: %v", id, r)
				}
				if id == 0 {
					h.StopTunnel(key)
				}
			}()
			h.runTunnel(tunnelCtx, serverHost, int(remotePort), token, int(localPort), localIP, tunnelPort)
		}(i)
	}
}

func (h *MessageHandler) runTunnel(ctx context.Context, serverHost string, remotePort int, token string, localPort int, localIP string, tunnelPort int) {
	log.Printf("Starting tunnel with multiplexing: remote=%s:%d, local=%s:%d, tunnelPort=%d",
		serverHost, remotePort, localIP, localPort, tunnelPort)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 使用隧道端口连接（如果有），否则使用代理端口
		connectPort := remotePort
		if tunnelPort > 0 {
			connectPort = tunnelPort
		}

		tunnelAddr := net.JoinHostPort(serverHost, fmt.Sprintf("%d", connectPort))
		log.Printf("Connecting tunnel to %s", tunnelAddr)

		startDial := time.Now()
		conn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
		dialElapsed := time.Since(startDial)
		if err != nil {
			log.Printf("Tunnel dial failed after %v: %v", dialElapsed, err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("Connected to tunnel from %s to %s (dial took %v)", conn.LocalAddr(), conn.RemoteAddr(), dialElapsed)

		start := time.Now()
		if err := proto.WriteTunnelAuth(conn, token); err != nil {
			log.Printf("Failed to send tunnel auth: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("Tunnel auth sent in %v, waiting for acknowledgement", time.Since(start))

		ack := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		ackN, err := io.ReadFull(conn, ack)
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			log.Printf("Acknowledgement read failed: %v (read %d bytes)", err, ackN)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		if ack[0] != 0x01 {
			log.Printf("Invalid acknowledgement byte: 0x%02x", ack[0])
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("Tunnel authenticated with ack=0x%02x, creating multiplexer", ack[0])

		mx := mux.New(conn)
		mx.LocalTarget = net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))
		mx.OnNewChannel = mx.HandleChannel

		<-mx.Done()
		log.Printf("Multiplexer closed, reconnecting...")

		time.Sleep(1 * time.Second)
	}
}

func (h *MessageHandler) runTunnelWS(ctx context.Context, serverHost string, token string, localPort int, localIP string, key string) {
	localTarget := net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))
	defer h.StopTunnel(key)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		scheme := "wss"
		if useHTTP {
			scheme = "ws"
		}
		tunnelURL := fmt.Sprintf("%s://%s:9909/tunnel", scheme, serverHost)
		log.Printf("Starting WebSocket tunnel: url=%s, local=%s", tunnelURL, localTarget)

		conn, err := h.connectTunnelWS(tunnelURL, token)
		if err != nil {
			log.Printf("WebSocket tunnel connect failed: %v, retrying...", err)
			time.Sleep(1 * time.Second)
			continue
		}

		mx := mux.New(conn)
		mx.LocalTarget = localTarget
		mx.OnNewChannel = mx.HandleChannel
		log.Printf("WebSocket tunnel multiplexer created, localTarget=%s", localTarget)

		<-mx.Done()
		log.Printf("WebSocket tunnel multiplexer closed, reconnecting...")
		time.Sleep(1 * time.Second)
	}
}

func (h *MessageHandler) connectTunnelWS(tunnelURL string, token string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !useHTTP {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if sni := tlsServerName(tunnelURL); sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !useHTTP {
		if origin := originHeader(tunnelURL); origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(tunnelURL, header)
	if err != nil {
		return nil, fmt.Errorf("tunnel ws dial: %w", err)
	}

	authMsg := map[string]interface{}{
		"type": "tunnel_auth",
		"data": map[string]string{
			"token": token,
		},
	}
	if err := ws.WriteJSON(authMsg); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel auth write: %w", err)
	}

	var resp map[string]interface{}
	if err := ws.ReadJSON(&resp); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel ready read: %w", err)
	}

	if respType, _ := resp["type"].(string); respType != "tunnel_ready" {
		ws.Close()
		return nil, fmt.Errorf("tunnel unexpected response: %v", resp)
	}

	log.Printf("WebSocket tunnel authenticated and ready")
	return wsconn.New(ws), nil
}
