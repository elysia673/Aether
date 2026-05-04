// Package handler processes messages received from the server
// and manages proxy tunnel lifecycles for TCP and WebSocket tunnels.
package handler

import (
	"Aether/pkg/model"
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
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageSender sends JSON messages through the WebSocket connection.
type MessageSender interface {
	WriteJSON(v interface{}) error
}

// Config holds configuration for tunnel connections.
type Config struct {
	ClientID       string
	UseHTTP        bool
	SNIOverride    string
	OriginOverride string
}

// Handler processes inbound server commands and manages tunnel instances.
type Handler struct {
	cfg        Config
	sender     MessageSender
	baseCtx    context.Context
	baseCancel context.CancelFunc
	tunnelMu   sync.Mutex
	tunnelCtxs map[string]context.CancelFunc
	p2p        *p2pManager
}

// New creates a new Handler.
func New(cfg Config) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		cfg:        cfg,
		baseCtx:    ctx,
		baseCancel: cancel,
		tunnelCtxs: make(map[string]context.CancelFunc),
	}
	p2pm := newP2PManager(cfg, ctx)
	h.p2p = p2pm
	return h
}

// SetSender sets the message sender (typically a conn.Connection).
func (h *Handler) SetSender(sender MessageSender) {
	h.sender = sender
	if h.p2p != nil {
		h.p2p.SetSender(sender)
	}
}

// Stop cancels all running tunnels.
func (h *Handler) Stop() {
	h.baseCancel()
	if h.p2p != nil {
		h.p2p.Close()
	}
}

// Handle dispatches incoming server messages by type.
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
	case "p2p_signal":
		if h.p2p != nil {
			h.p2p.handleP2PSignal(msg.Data)
		}
	case "p2p_closed":
		if h.p2p != nil {
			h.p2p.handleP2PClosed(msg.Data)
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
			MinVersion: tls.VersionTLS12,
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
