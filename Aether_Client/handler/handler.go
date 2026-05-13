// Package handler 处理从服务器接收的消息，并管理 TCP 和 WebSocket 隧道的生命周期。
package handler

import (
	"Aether/common/model"
	"Aether/common/mux"
	"Aether/common/proto"
	"Aether/common/wsconn"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageSender 通过 WebSocket 连接发送 JSON 消息。
type MessageSender interface {
	WriteJSON(v interface{}) error
}

// Config 隧道连接配置。
type Config struct {
	ClientID       string
	UseHTTP        bool
	Insecure       bool
	SNIOverride    string
	OriginOverride string
}

// Handler 处理入站服务器命令并管理隧道实例。
type Handler struct {
	cfg        Config
	sender     MessageSender
	baseCtx    context.Context
	baseCancel context.CancelFunc
	tunnelMu   sync.Mutex
	tunnelCtxs map[string]context.CancelFunc
	relay      *relayManager
}

// New 创建新的 Handler。
func New(cfg Config) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		cfg:        cfg,
		baseCtx:    ctx,
		baseCancel: cancel,
		tunnelCtxs: make(map[string]context.CancelFunc),
	}
	relayMgr := newRelayManager(cfg, ctx)
	h.relay = relayMgr
	return h
}

// SetSender 设置消息发送器（通常是 conn.Connection）。
func (h *Handler) SetSender(sender MessageSender) {
	h.sender = sender
	if h.relay != nil {
		h.relay.SetSender(sender)
	}
}

// Stop 取消所有正在运行的隧道。
func (h *Handler) Stop() {
	h.baseCancel()
	if h.relay != nil {
		h.relay.Close()
	}
}

// Handle 根据类型分发入站服务器消息。
func (h *Handler) Handle(msg *model.WSMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in handler.Handle: %v", r)
		}
	}()
	switch msg.Type {
	case "proxy":
		h.handleProxy(msg.Data)
	case "proxy_closed":
		h.handleProxyClosed(msg.Data)
	case "relay_signal":
		if h.relay != nil {
			h.relay.handleRelaySignal(msg.Data)
		}
	case "relay_closed":
		if h.relay != nil {
			h.relay.handleRelayClosed(msg.Data)
		}
	}
}

func (h *Handler) registerTunnel(key string, cancel context.CancelFunc) {
	h.tunnelMu.Lock()
	h.tunnelCtxs[key] = cancel
	h.tunnelMu.Unlock()
}

func (h *Handler) stopTunnel(key string) {
	h.tunnelMu.Lock()
	if cancel, ok := h.tunnelCtxs[key]; ok {
		cancel()
		delete(h.tunnelCtxs, key)
	}
	h.tunnelMu.Unlock()
}

func (h *Handler) handleProxyClosed(data interface{}) {
	closed, err := unmarshalData[model.ProxyClosedData](data)
	if err != nil {
		log.Printf("proxy_closed unmarshal error: %v", err)
		return
	}
	log.Printf("proxy closed by server: %s, stopping tunnel", closed.Key)
	h.stopTunnel(closed.Key)
}

func (h *Handler) handleProxy(data interface{}) {
	cmd, err := unmarshalData[model.CommandData](data)
	if err != nil {
		log.Printf("proxy message unmarshal error: %v", err)
		return
	}

	if cmd.LocalIP == "" {
		cmd.LocalIP = "127.0.0.1"
	}

	log.Printf("proxy command: serverHost=%s, remotePort=%d, localPort=%d, localIP=%s, token=%s, protocol=%s, tunnelPort=%d",
		cmd.ServerHost, cmd.RemotePort, cmd.LocalPort, cmd.LocalIP, cmd.Token, cmd.Protocol, cmd.TunnelPort)

	key := fmt.Sprintf("%s-%d", h.cfg.ClientID, cmd.RemotePort)

	if cmd.Protocol == "websocket" {
		tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
		h.registerTunnel(key, tunnelCancel)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC in WS tunnel: %v", r)
				}
				h.stopTunnel(key)
			}()
			h.runTunnelWS(tunnelCtx, cmd.ServerHost, cmd.Token, cmd.LocalPort, cmd.LocalIP, key)
		}()
		return
	}

	if cmd.Protocol == "udp" {
		tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
		h.registerTunnel(key, tunnelCancel)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC in UDP tunnel: %v", r)
				}
				h.stopTunnel(key)
			}()
			h.runTunnelUDP(tunnelCtx, cmd.ServerHost, cmd.RemotePort, cmd.Token, cmd.LocalPort, cmd.LocalIP)
		}()
		return
	}

	tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
	h.registerTunnel(key, tunnelCancel)
	const tunnelPoolSize = 1
	for i := 0; i < tunnelPoolSize; i++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC in tunnel goroutine %d: %v", id, r)
				}
				if id == 0 {
					h.stopTunnel(key)
				}
			}()
			h.runTunnel(tunnelCtx, cmd.ServerHost, cmd.RemotePort, cmd.Token, cmd.LocalPort, cmd.LocalIP, cmd.TunnelPort)
		}(i)
	}
}

func (h *Handler) runTunnel(ctx context.Context, serverHost string, remotePort int, token string, localPort int, localIP string, tunnelPort int) {
	log.Printf("Starting tunnel with multiplexing: remote=%s:%d, local=%s:%d, tunnelPort=%d",
		serverHost, remotePort, localIP, localPort, tunnelPort)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

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

// runTunnelUDP 运行 UDP 隧道
// 通过 TCP 连接到服务器的 UDP 隧道端口，然后转发 UDP 数据
func (h *Handler) runTunnelUDP(ctx context.Context, serverHost string, remotePort int, token string, localPort int, localIP string) {
	log.Printf("启动 UDP 隧道: 远程=%s:%d, 本地=%s:%d", serverHost, remotePort, localIP, localPort)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tunnelAddr := net.JoinHostPort(serverHost, fmt.Sprintf("%d", remotePort))
		log.Printf("连接 UDP 隧道到 %s", tunnelAddr)

		conn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
		if err != nil {
			log.Printf("UDP 隧道连接失败: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("已连接到 UDP 隧道 %s -> %s", conn.LocalAddr(), conn.RemoteAddr())

		// 发送认证标记 "TUNNEL\n"
		if _, err := conn.Write([]byte("TUNNEL\n")); err != nil {
			log.Printf("UDP 隧道认证标记发送失败: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 发送 token 长度和 token
		tokenBytes := []byte(token)
		tokenLen := uint16(len(tokenBytes))
		if err := binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
			log.Printf("UDP 隧道 token 长度发送失败: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		if _, err := conn.Write(tokenBytes); err != nil {
			log.Printf("UDP 隧道 token 发送失败: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 等待确认
		ack := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.ReadFull(conn, ack); err != nil {
			log.Printf("UDP 隧道确认读取失败: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		conn.SetReadDeadline(time.Time{})

		if ack[0] != 0x01 {
			log.Printf("UDP 隧道确认无效: 0x%02x", ack[0])
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("UDP 隧道认证成功")

		// 启动 UDP 数据转发
		h.handleUDPTunnel(conn, localIP, localPort)

		log.Printf("UDP 隧道断开，正在重连...")
		time.Sleep(1 * time.Second)
	}
}

// handleUDPTunnel 处理 UDP 隧道数据转发
func (h *Handler) handleUDPTunnel(conn net.Conn, localIP string, localPort int) {
	defer conn.Close()

	// 连接到本地 UDP 服务
	localAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort)))
	if err != nil {
		log.Printf("本地 UDP 地址解析失败: %v", err)
		return
	}

	// 用于发送 UDP 数据到本地服务
	// 注意：UDP 是无连接的，每次发送可能需要不同的源地址
	// 这里我们使用一个固定的 UDP 连接来发送
	localConn, err := net.DialUDP("udp", nil, localAddr)
	if err != nil {
		log.Printf("本地 UDP 连接失败: %v", err)
		return
	}
	defer localConn.Close()

	// 用于跟踪 UDP 会话（本地端口 -> 远程地址）
	type udpSession struct {
		remoteAddr *net.UDPAddr
		lastActive time.Time
	}
	sessions := make(map[uint16]*udpSession)
	var sessionsMu sync.Mutex

	// 清理过期会话
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			sessionsMu.Lock()
			now := time.Now()
			for port, sess := range sessions {
				if now.Sub(sess.lastActive) > 60*time.Second {
					delete(sessions, port)
				}
			}
			sessionsMu.Unlock()
		}
	}()

	// 从服务器读取 UDP 数据并转发到本地
	go func() {
		for {
			// 读取 UDP 数据头：[2字节目标端口][2字节数据长度]
			var destPort uint16
			var dataLen uint16

			if err := binary.Read(conn, binary.BigEndian, &destPort); err != nil {
				if err != io.EOF {
					log.Printf("UDP 隧道读取端口错误: %v", err)
				}
				return
			}

			if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
				log.Printf("UDP 隧道读取长度错误: %v", err)
				return
			}

			if dataLen > 65535 {
				log.Printf("UDP 数据包过大: %d", dataLen)
				return
			}

			data := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, data); err != nil {
				log.Printf("UDP 隧道读取数据错误: %v", err)
				return
			}

			// 更新会话
			sessionsMu.Lock()
			sessions[destPort] = &udpSession{
				lastActive: time.Now(),
			}
			sessionsMu.Unlock()

			// 发送到本地 UDP 服务
			if _, err := localConn.Write(data); err != nil {
				log.Printf("本地 UDP 发送错误: %v", err)
				continue
			}
		}
	}()

	// 从本地 UDP 服务读取响应并发送回服务器
	buf := make([]byte, 65535)
	for {
		n, err := localConn.Read(buf)
		if err != nil {
			log.Printf("本地 UDP 读取错误: %v", err)
			break
		}

		if n == 0 {
			continue
		}

		// 获取本地端口（作为源端口）
		localPort := localConn.LocalAddr().(*net.UDPAddr).Port

		// 发送响应到服务器
		// 格式：[2字节源端口][2字节数据长度][数据]
		packet := make([]byte, 4+n)
		binary.BigEndian.PutUint16(packet[0:2], uint16(localPort))
		binary.BigEndian.PutUint16(packet[2:4], uint16(n))
		copy(packet[4:], buf[:n])

		if _, err := conn.Write(packet); err != nil {
			log.Printf("UDP 隧道写入错误: %v", err)
			break
		}
	}
}

func (h *Handler) runTunnelWS(ctx context.Context, serverHost string, token string, localPort int, localIP string, key string) {
	localTarget := net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		scheme := "wss"
		if h.cfg.UseHTTP {
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

func (h *Handler) connectTunnelWS(tunnelURL string, token string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !h.cfg.UseHTTP {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: h.cfg.Insecure,
		}
		sni := h.sniForURL(tunnelURL)
		if sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !h.cfg.UseHTTP {
		origin := h.originForURL(tunnelURL)
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(tunnelURL, header)
	if err != nil {
		return nil, fmt.Errorf("tunnel ws dial: %w", err)
	}

	authMsg := model.WSMessage{
		Type: "tunnel_auth",
		Data: model.TunnelAuthData{Token: token},
	}
	if err := ws.WriteJSON(authMsg); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel auth write: %w", err)
	}

	var resp model.TunnelReadyMsg
	if err := ws.ReadJSON(&resp); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel ready read: %w", err)
	}

	if resp.Type != "tunnel_ready" {
		ws.Close()
		return nil, fmt.Errorf("tunnel unexpected response type: %s", resp.Type)
	}

	log.Printf("WebSocket tunnel authenticated and ready")
	return wsconn.New(ws), nil
}

func (h *Handler) sniForURL(rawURL string) string {
	if h.cfg.SNIOverride != "" {
		return h.cfg.SNIOverride
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func (h *Handler) originForURL(rawURL string) string {
	if h.cfg.OriginOverride != "" {
		return h.cfg.OriginOverride
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" {
		return ""
	}
	if h.cfg.UseHTTP {
		return "http://" + host
	}
	return "https://" + host
}

func unmarshalData[T any](data interface{}) (*T, error) {
	switch v := data.(type) {
	case string:
		var result T
		if err := json.Unmarshal([]byte(v), &result); err != nil {
			return nil, fmt.Errorf("unmarshal string data: %w", err)
		}
		return &result, nil
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal data: %w", err)
		}
		var result T
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
		return &result, nil
	}
}
