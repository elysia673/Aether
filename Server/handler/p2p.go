package handler

import (
	"Aether/Server/manager"
	"Aether/pkg/model"
	"Aether/tools/wsconn"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type P2PHandler struct {
	clientMgr  *manager.ClientManager
	serverHost string

	mu       sync.Mutex
	sessions map[string]*p2pSessionState
}

type p2pSessionState struct {
	ID            string
	SourceClient  string
	TargetClient  string
	Protocol      string
	SourcePort    int
	TargetPort    int
	TargetLocalIP string
	SourceLocalIP string
	Token         string
	CreatedAt     time.Time
	SourceReady   bool
	TargetReady   bool
	Error         string
	sourceConn    *websocket.Conn
	targetConn    *websocket.Conn
	done          chan struct{}
	once          sync.Once
}

func NewP2PHandler(clientMgr *manager.ClientManager, serverHost string) *P2PHandler {
	return &P2PHandler{
		clientMgr:  clientMgr,
		serverHost: serverHost,
		sessions:   make(map[string]*p2pSessionState),
	}
}

type p2pConnectRequest struct {
	SourceClientID string `json:"source_client_id" binding:"required"`
	TargetClientID string `json:"target_client_id" binding:"required"`
	TargetPort     int    `json:"target_port" binding:"required,min=1,max=65535"`
	SourcePort     int    `json:"source_port" binding:"required,min=1,max=65535"`
	Protocol       string `json:"protocol" binding:"required,oneof=udp tcp websocket"`
	TargetLocalIP  string `json:"target_local_ip"`
	SourceLocalIP  string `json:"source_local_ip"`
}

func (h *P2PHandler) CreateP2P(c *gin.Context) {
	var req p2pConnectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	if req.SourceClientID == req.TargetClientID {
		c.JSON(http.StatusBadRequest, model.Error(400, "source and target clients must be different"))
		return
	}

	_, ok := h.clientMgr.Get(req.SourceClientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "source client not found"))
		return
	}

	_, ok = h.clientMgr.Get(req.TargetClientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "target client not found"))
		return
	}

	if req.TargetLocalIP == "" {
		req.TargetLocalIP = "127.0.0.1"
	}
	if req.SourceLocalIP == "" {
		req.SourceLocalIP = "0.0.0.0"
	}

	sessionID := generateSessionID()
	token := generateToken()

	serverHost := h.serverHost
	if serverHost == "" {
		serverHost = h.clientMgr.GetPublicIP()
	}

	session := &p2pSessionState{
		ID:            sessionID,
		SourceClient:  req.SourceClientID,
		TargetClient:  req.TargetClientID,
		Protocol:      req.Protocol,
		SourcePort:    req.SourcePort,
		TargetPort:    req.TargetPort,
		TargetLocalIP: req.TargetLocalIP,
		SourceLocalIP: req.SourceLocalIP,
		Token:         token,
		CreatedAt:     time.Now(),
		done:          make(chan struct{}),
	}

	h.mu.Lock()
	h.sessions[sessionID] = session
	h.mu.Unlock()

	signal := model.WSMessage{
		Type: "p2p_signal",
		Data: model.P2PSignalData{
			SessionID:     sessionID,
			Protocol:      req.Protocol,
			Role:          "",
			PeerClientID:  "",
			SourcePort:    req.SourcePort,
			TargetPort:    req.TargetPort,
			TargetLocalIP: req.TargetLocalIP,
			SourceLocalIP: req.SourceLocalIP,
			ServerHost:    serverHost,
			Token:         token,
		},
	}

	sourceSignal := signal
	sourceSignalData := sourceSignal.Data.(model.P2PSignalData)
	sourceSignalData.Role = "source"
	sourceSignalData.PeerClientID = req.TargetClientID
	sourceSignal.Data = sourceSignalData

	targetSignal := signal
	targetSignalData := targetSignal.Data.(model.P2PSignalData)
	targetSignalData.Role = "target"
	targetSignalData.PeerClientID = req.SourceClientID
	targetSignal.Data = targetSignalData

	if err := h.clientMgr.SendCommand(req.SourceClientID, sourceSignal); err != nil {
		h.cleanupSession(sessionID)
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send signal to source: "+err.Error()))
		return
	}
	if err := h.clientMgr.SendCommand(req.TargetClientID, targetSignal); err != nil {
		h.cleanupSession(sessionID)
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send signal to target: "+err.Error()))
		return
	}

	go h.monitorSession(session)

	log.Printf("P2P relay session %s: %s <-> %s, protocol=%s",
		sessionID, req.SourceClientID, req.TargetClientID, req.Protocol)

	c.JSON(http.StatusOK, model.Success(map[string]interface{}{
		"session_id":    sessionID,
		"source_client": req.SourceClientID,
		"target_client": req.TargetClientID,
		"protocol":      req.Protocol,
	}))
}

func (h *P2PHandler) HandleRelayWS(c *gin.Context) {
	sessionID := c.Query("session")
	token := c.Query("token")
	role := c.Query("role")

	if sessionID == "" || token == "" || role == "" {
		http.Error(c.Writer, "missing params", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	session, ok := h.sessions[sessionID]
	h.mu.Unlock()

	if !ok {
		http.Error(c.Writer, "session not found", http.StatusNotFound)
		return
	}
	if session.Token != token {
		http.Error(c.Writer, "invalid token", http.StatusForbidden)
		return
	}

	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("P2P relay WS upgrade error: %v", err)
		return
	}

	if role == "source" {
		session.sourceConn = ws
	} else {
		session.targetConn = ws
	}

	log.Printf("P2P relay: %s connected (session=%s)", role, sessionID)

	if session.sourceConn != nil && session.targetConn != nil {
		go h.bridgeRelay(session)
	}
}

func (h *P2PHandler) bridgeRelay(session *p2pSessionState) {
	defer h.cleanupSession(session.ID)

	src := wsconn.New(session.sourceConn)
	dst := wsconn.New(session.targetConn)
	defer src.Close()
	defer dst.Close()

	log.Printf("P2P relay: bridging %s", session.ID)

	done := make(chan struct{}, 2)
	go func() {
		io.Copy(dst, src)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(src, dst)
		done <- struct{}{}
	}()

	<-done
	log.Printf("P2P relay: session %s closed", session.ID)
}

func (h *P2PHandler) monitorSession(session *p2pSessionState) {
	select {
	case <-session.done:
	case <-time.After(120 * time.Second):
		if session.sourceConn == nil || session.targetConn == nil {
			log.Printf("P2P session %s: relay timeout", session.ID)
			h.cleanupSession(session.ID)
		}
	}
}

func (h *P2PHandler) HandleClientStatus(msg *model.WSMessage, clientID string) {
	switch msg.Type {
	case "p2p_established":
		var status model.P2PStatusData
		data, _ := json.Marshal(msg.Data)
		json.Unmarshal(data, &status)

		h.mu.Lock()
		session, ok := h.sessions[status.SessionID]
		h.mu.Unlock()
		if !ok {
			return
		}

		if status.Status == "failed" {
			log.Printf("P2P session %s: client %s reported failure: %s", status.SessionID, clientID, status.Message)
			session.Error = clientID + ": " + status.Message
			session.done <- struct{}{}
			go func() {
				time.Sleep(30 * time.Second)
				h.cleanupSession(status.SessionID)
			}()
			return
		}

		if clientID == session.SourceClient {
			session.SourceReady = true
		} else if clientID == session.TargetClient {
			session.TargetReady = true
		}

	case "p2p_closed":
		var status model.P2PStatusData
		data, _ := json.Marshal(msg.Data)
		json.Unmarshal(data, &status)
		h.cleanupSession(status.SessionID)
	}
}

func (h *P2PHandler) cleanupSession(sessionID string) {
	h.mu.Lock()
	session, ok := h.sessions[sessionID]
	if ok {
		delete(h.sessions, sessionID)
	}
	h.mu.Unlock()

	if ok {
		session.once.Do(func() {
			close(session.done)
			if session.sourceConn != nil {
				session.sourceConn.Close()
			}
			if session.targetConn != nil {
				session.targetConn.Close()
			}
		})
		log.Printf("P2P session %s cleaned up", sessionID)
	}
}

func (h *P2PHandler) ListSessions(c *gin.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sessions := make([]map[string]interface{}, 0)
	for _, s := range h.sessions {
		status := "connecting"
		if s.Error != "" {
			status = "failed"
		} else if s.sourceConn != nil && s.targetConn != nil {
			status = "connected"
		}
		entry := map[string]interface{}{
			"session_id":    s.ID,
			"source_client": s.SourceClient,
			"target_client": s.TargetClient,
			"protocol":      s.Protocol,
			"source_port":   s.SourcePort,
			"target_port":   s.TargetPort,
			"status":        status,
			"created_at":    s.CreatedAt.Unix(),
		}
		if s.Error != "" {
			entry["error"] = s.Error
		}
		sessions = append(sessions, entry)
	}
	c.JSON(http.StatusOK, model.Success(map[string]interface{}{"sessions": sessions}))
}

func (h *P2PHandler) CloseSession(c *gin.Context) {
	sessionID := c.Param("id")
	h.cleanupSession(sessionID)
	c.JSON(http.StatusOK, model.Success(map[string]string{"session_id": sessionID}))
}

func generateSessionID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
