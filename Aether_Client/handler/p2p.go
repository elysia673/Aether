package handler

import (
	"Aether/common/model"
	"Aether/common/mux"
	"Aether/common/wsconn"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// p2pSession 表示一个 P2P 会话。
type p2pSession struct {
	id            string
	protocol      string
	role          string
	targetPort    int
	targetLocalIP string
	sourceLocalIP string
	sourcePort    int
	token         string
	serverHost    string
	cancel        context.CancelFunc
	mx            *mux.Multiplexer
	ln            net.Listener
}

// p2pManager 管理客户端 P2P 会话。
type p2pManager struct {
	mu       sync.Mutex
	sessions map[string]*p2pSession
	sender   MessageSender
	cfg      Config
	baseCtx  context.Context
}

// newP2PManager 创建新的 P2P 管理器。
func newP2PManager(cfg Config, baseCtx context.Context) *p2pManager {
	return &p2pManager{
		sessions: make(map[string]*p2pSession),
		cfg:      cfg,
		baseCtx:  baseCtx,
	}
}

// SetSender 设置消息发送器。
func (pm *p2pManager) SetSender(sender MessageSender) {
	pm.sender = sender
}

// handleP2PSignal 处理 P2P 信令消息。
func (pm *p2pManager) handleP2PSignal(data interface{}) {
	sig, err := unmarshalData[model.P2PSignalData](data)
	if err != nil {
		log.Printf("P2P signal unmarshal error: %v", err)
		return
	}

	log.Printf("P2P signal: session=%s role=%s protocol=%s source_port=%d target_port=%d server=%s",
		sig.SessionID, sig.Role, sig.Protocol, sig.SourcePort, sig.TargetPort, sig.ServerHost)

	_, cancel := context.WithCancel(pm.baseCtx)

	sess := &p2pSession{
		id:            sig.SessionID,
		protocol:      sig.Protocol,
		role:          sig.Role,
		targetPort:    sig.TargetPort,
		targetLocalIP: sig.TargetLocalIP,
		sourceLocalIP: sig.SourceLocalIP,
		sourcePort:    sig.SourcePort,
		token:         sig.Token,
		serverHost:    sig.ServerHost,
		cancel:        cancel,
	}

	pm.mu.Lock()
	pm.sessions[sig.SessionID] = sess
	pm.mu.Unlock()

	if sig.Role == "source" {
		go pm.runSource(sess)
	} else {
		go pm.runTarget(sess)
	}
}

// handleP2PClosed 处理 P2P 会话关闭消息。
func (pm *p2pManager) handleP2PClosed(data interface{}) {
	status, err := unmarshalData[model.P2PStatusData](data)
	if err != nil {
		return
	}

	pm.mu.Lock()
	sess, ok := pm.sessions[status.SessionID]
	if ok {
		delete(pm.sessions, status.SessionID)
	}
	pm.mu.Unlock()

	if ok {
		sess.cancel()
	}
}

func (pm *p2pManager) stop() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, s := range pm.sessions {
		s.cancel()
	}
	pm.sessions = make(map[string]*p2pSession)
}

// Close 关闭所有 P2P 会话。
func (pm *p2pManager) Close() {
	pm.stop()
}

func (pm *p2pManager) sendEstablished(sessionID string, connected bool, msg string) {
	if pm.sender == nil {
		return
	}
	status := "connected"
	if !connected {
		status = "failed"
	}
	sendData, _ := json.Marshal(model.P2PStatusData{
		SessionID: sessionID, Status: status, Message: msg,
	})
	pm.sender.WriteJSON(&model.WSMessage{Type: "p2p_established", Data: string(sendData)})
}

func (pm *p2pManager) sendClosed(sessionID string) {
	if pm.sender == nil {
		return
	}
	sendData, _ := json.Marshal(model.P2PStatusData{SessionID: sessionID, Status: "closed"})
	pm.sender.WriteJSON(&model.WSMessage{Type: "p2p_closed", Data: string(sendData)})
}

func (pm *p2pManager) cleanupSession(sessionID string) {
	pm.mu.Lock()
	sess, ok := pm.sessions[sessionID]
	if ok {
		delete(pm.sessions, sessionID)
	}
	pm.mu.Unlock()
	if ok {
		sess.cancel()
		if sess.ln != nil {
			sess.ln.Close()
		}
		if sess.mx != nil {
			sess.mx.Close()
		}
	}
}

func (pm *p2pManager) runSource(sess *p2pSession) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in P2P source %s: %v", sess.id, r)
		}
		pm.cleanupSession(sess.id)
		pm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if pm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/p2p-relay?session=%s&token=%s&role=source&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, pm.cfg.ClientID)

	log.Printf("P2P source: connecting to relay %s", relayURL)

	conn, err := pm.connectRelay(relayURL)
	if err != nil {
		log.Printf("P2P source: relay connect failed: %v", err)
		pm.sendEstablished(sess.id, false, err.Error())
		return
	}

	mx := mux.New(conn)
	sess.mx = mx

	bindAddr := net.JoinHostPort(sess.sourceLocalIP, fmt.Sprintf("%d", sess.sourcePort))
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Printf("P2P source: listen %s failed: %v", bindAddr, err)
		conn.Close()
		pm.sendEstablished(sess.id, false, err.Error())
		return
	}
	sess.ln = ln
	defer ln.Close()

	pm.sendEstablished(sess.id, true, "")
	log.Printf("P2P source: listening on %s", bindAddr)

	go func() {
		<-mx.Done()
		ln.Close()
	}()

	for {
		localConn, err := ln.Accept()
		if err != nil {
			return
		}
		channel, err := mx.CreateChannel()
		if err != nil {
			localConn.Close()
			return
		}
		log.Printf("P2P source: new connection, channel %d", channel.ID)
		go bridgeChannel(localConn, channel, sess.id)
	}
}

func (pm *p2pManager) runTarget(sess *p2pSession) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in P2P target %s: %v", sess.id, r)
		}
		pm.cleanupSession(sess.id)
		pm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if pm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/p2p-relay?session=%s&token=%s&role=target&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, pm.cfg.ClientID)

	log.Printf("P2P target: connecting to relay %s", relayURL)

	conn, err := pm.connectRelay(relayURL)
	if err != nil {
		log.Printf("P2P target: relay connect failed: %v", err)
		pm.sendEstablished(sess.id, false, err.Error())
		return
	}

	localTarget := net.JoinHostPort(sess.targetLocalIP, fmt.Sprintf("%d", sess.targetPort))
	mx := mux.New(conn)
	mx.LocalTarget = localTarget
	mx.OnNewChannel = mx.HandleChannel
	sess.mx = mx

	pm.sendEstablished(sess.id, true, "")
	log.Printf("P2P target: relay ready, localTarget=%s", localTarget)

	<-mx.Done()
	log.Printf("P2P target: session %s closed", sess.id)
}

func (pm *p2pManager) connectRelay(relayURL string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	u, _ := url.Parse(relayURL)
	hostname := ""
	if u != nil {
		hostname = u.Hostname()
	}

	if !pm.cfg.UseHTTP {
		dialer.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		if pm.cfg.SNIOverride != "" {
			dialer.TLSClientConfig.ServerName = pm.cfg.SNIOverride
		} else if hostname != "" {
			dialer.TLSClientConfig.ServerName = hostname
		}
	}

	header := http.Header{}
	if !pm.cfg.UseHTTP {
		origin := pm.cfg.OriginOverride
		if origin == "" && hostname != "" {
			origin = "https://" + hostname
		}
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(relayURL, header)
	if err != nil {
		return nil, fmt.Errorf("relay dial: %w", err)
	}
	return wsconn.New(ws), nil
}

// bridgeChannel 在本地连接和多路复用通道之间桥接数据。
func bridgeChannel(localConn net.Conn, channel *mux.Channel, sessionID string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in bridgeChannel %s ch%d: %v", sessionID, channel.ID, r)
		}
		localConn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 65536)
		for {
			n, err := localConn.Read(buf)
			if err != nil {
				break
			}
			if err := channel.Mux.Send(channel.ID, buf[:n]); err != nil {
				break
			}
		}
		channel.Mux.CloseChannel(channel.ID)
	}()

	go func() {
		defer wg.Done()
		for {
			data, ok := channel.ReceiveBlocking()
			if !ok {
				break
			}
			if _, err := localConn.Write(data); err != nil {
				break
			}
		}
	}()

	wg.Wait()
}
