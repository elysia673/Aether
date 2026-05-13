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

// relaySession 表示一个中继会话。
type relaySession struct {
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

// relayManager 管理客户端中继会话。
type relayManager struct {
	mu       sync.Mutex
	sessions map[string]*relaySession
	sender   MessageSender
	cfg      Config
	baseCtx  context.Context
}

// newRelayManager 创建新的中继管理器。
func newRelayManager(cfg Config, baseCtx context.Context) *relayManager {
	return &relayManager{
		sessions: make(map[string]*relaySession),
		cfg:      cfg,
		baseCtx:  baseCtx,
	}
}

// SetSender 设置消息发送器。
func (rm *relayManager) SetSender(sender MessageSender) {
	rm.sender = sender
}

// handleRelaySignal 处理中继信令消息。
func (rm *relayManager) handleRelaySignal(data interface{}) {
	sig, err := unmarshalData[model.RelaySignalData](data)
	if err != nil {
		log.Printf("中继信令解析错误: %v", err)
		return
	}

	log.Printf("中继信令: 会话=%s 角色=%s 协议=%s 源端口=%d 目标端口=%d 服务器=%s",
		sig.SessionID, sig.Role, sig.Protocol, sig.SourcePort, sig.TargetPort, sig.ServerHost)

	_, cancel := context.WithCancel(rm.baseCtx)

	sess := &relaySession{
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

	rm.mu.Lock()
	rm.sessions[sig.SessionID] = sess
	rm.mu.Unlock()

	if sig.Role == "source" {
		go rm.runSource(sess)
	} else {
		go rm.runTarget(sess)
	}
}

// handleRelayClosed 处理中继会话关闭消息。
func (rm *relayManager) handleRelayClosed(data interface{}) {
	status, err := unmarshalData[model.RelayStatusData](data)
	if err != nil {
		return
	}

	rm.mu.Lock()
	sess, ok := rm.sessions[status.SessionID]
	if ok {
		delete(rm.sessions, status.SessionID)
	}
	rm.mu.Unlock()

	if ok {
		sess.cancel()
	}
}

// stop 停止所有中继会话。
func (rm *relayManager) stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, s := range rm.sessions {
		s.cancel()
	}
	rm.sessions = make(map[string]*relaySession)
}

// Close 关闭所有中继会话。
func (rm *relayManager) Close() {
	rm.stop()
}

// sendEstablished 发送中继建立状态。
func (rm *relayManager) sendEstablished(sessionID string, connected bool, msg string) {
	if rm.sender == nil {
		return
	}
	status := "connected"
	if !connected {
		status = "failed"
	}
	sendData, _ := json.Marshal(model.RelayStatusData{
		SessionID: sessionID, Status: status, Message: msg,
	})
	rm.sender.WriteJSON(&model.WSMessage{Type: "relay_established", Data: string(sendData)})
}

// sendClosed 发送中继关闭状态。
func (rm *relayManager) sendClosed(sessionID string) {
	if rm.sender == nil {
		return
	}
	sendData, _ := json.Marshal(model.RelayStatusData{SessionID: sessionID, Status: "closed"})
	rm.sender.WriteJSON(&model.WSMessage{Type: "relay_closed", Data: string(sendData)})
}

// cleanupSession 清理中继会话。
func (rm *relayManager) cleanupSession(sessionID string) {
	rm.mu.Lock()
	sess, ok := rm.sessions[sessionID]
	if ok {
		delete(rm.sessions, sessionID)
	}
	rm.mu.Unlock()
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

// runSource 运行源端中继。
func (rm *relayManager) runSource(sess *relaySession) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("中继源端 %s panic: %v", sess.id, r)
		}
		rm.cleanupSession(sess.id)
		rm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if rm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/relay?session=%s&token=%s&role=source&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, rm.cfg.ClientID)

	log.Printf("中继源端: 正在连接中继 %s", relayURL)

	conn, err := rm.connectRelay(relayURL)
	if err != nil {
		log.Printf("中继源端: 中继连接失败: %v", err)
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}

	mx := mux.New(conn)
	sess.mx = mx

	bindAddr := net.JoinHostPort(sess.sourceLocalIP, fmt.Sprintf("%d", sess.sourcePort))
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Printf("中继源端: 监听 %s 失败: %v", bindAddr, err)
		conn.Close()
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}
	sess.ln = ln
	defer ln.Close()

	rm.sendEstablished(sess.id, true, "")
	log.Printf("中继源端: 正在监听 %s", bindAddr)

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
		log.Printf("中继源端: 新连接，通道 %d", channel.ID)
		go bridgeChannel(localConn, channel, sess.id)
	}
}

// runTarget 运行目标端中继。
func (rm *relayManager) runTarget(sess *relaySession) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("中继目标端 %s panic: %v", sess.id, r)
		}
		rm.cleanupSession(sess.id)
		rm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if rm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/relay?session=%s&token=%s&role=target&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, rm.cfg.ClientID)

	log.Printf("中继目标端: 正在连接中继 %s", relayURL)

	conn, err := rm.connectRelay(relayURL)
	if err != nil {
		log.Printf("中继目标端: 中继连接失败: %v", err)
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}

	localTarget := net.JoinHostPort(sess.targetLocalIP, fmt.Sprintf("%d", sess.targetPort))
	mx := mux.New(conn)
	mx.LocalTarget = localTarget
	mx.OnNewChannel = mx.HandleChannel
	sess.mx = mx

	rm.sendEstablished(sess.id, true, "")
	log.Printf("中继目标端: 中继就绪，本地目标=%s", localTarget)

	<-mx.Done()
	log.Printf("中继目标端: 会话 %s 已关闭", sess.id)
}

// connectRelay 连接到中继服务器。
func (rm *relayManager) connectRelay(relayURL string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	u, _ := url.Parse(relayURL)
	hostname := ""
	if u != nil {
		hostname = u.Hostname()
	}

	if !rm.cfg.UseHTTP {
		dialer.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: rm.cfg.Insecure}
		if rm.cfg.SNIOverride != "" {
			dialer.TLSClientConfig.ServerName = rm.cfg.SNIOverride
		} else if hostname != "" {
			dialer.TLSClientConfig.ServerName = hostname
		}
	}

	header := http.Header{}
	if !rm.cfg.UseHTTP {
		origin := rm.cfg.OriginOverride
		if origin == "" && hostname != "" {
			origin = "https://" + hostname
		}
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(relayURL, header)
	if err != nil {
		return nil, fmt.Errorf("中继拨号: %w", err)
	}
	return wsconn.New(ws), nil
}

// bridgeChannel 在本地连接和多路复用通道之间桥接数据。
func bridgeChannel(localConn net.Conn, channel *mux.Channel, sessionID string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("bridgeChannel %s 通道%d panic: %v", sessionID, channel.ID, r)
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
