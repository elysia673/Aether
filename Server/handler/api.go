package handler

import (
	"Aether/Server/manager"
	"Aether/Server/model"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// APIHandler API处理器
type APIHandler struct {
	clientMgr      *manager.ClientManager
	tunnelMgr      *manager.TunnelManager
	proxyPortMap   map[int]int // remotePort -> localPort
	proxyPortLock  sync.RWMutex
	proxyListeners map[int]net.Listener // remotePort -> listener，便于关闭
	listenerLock   sync.Mutex
}

// NewAPIHandler 创建API处理器实例
func NewAPIHandler(clientMgr *manager.ClientManager, tunnelMgr *manager.TunnelManager) *APIHandler {
	return &APIHandler{
		clientMgr:      clientMgr,
		tunnelMgr:      tunnelMgr,
		proxyPortMap:   make(map[int]int),
		proxyListeners: make(map[int]net.Listener),
	}
}

// ListClients 获取所有在线客户端列表
func (h *APIHandler) ListClients(c *gin.Context) {
	clients := h.clientMgr.ListClients()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"clients": clients,
	}))
}

// POST /api/v1/clients/:id/proxy
// proxyRequest 创建代理请求参数
type proxyRequest struct {
	RemotePort int `json:"remote_port" binding:"required,min=1,max=65535"`
	LocalPort  int `json:"local_port" binding:"required,min=1,max=65535"`
}

// CreateProxy 为指定客户端创建端口代理
func (h *APIHandler) CreateProxy(c *gin.Context) {
	// 从 URL 路径参数获取 id
	clientID := c.Param("id")
	var req proxyRequest
	// 解析请求体 JSON 并绑定到 req
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	requestID := uuid.New().String()
	cmdData, _ := json.Marshal(model.CommandData{
		RequestID:  requestID,
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
	})
	cmd := model.WSMessage{
		Type: "proxy",
		Data: string(cmdData),
	}

	if err := h.clientMgr.SendCommand(clientID, cmd); err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send command: "+err.Error()))
		return
	}

	// 记录端口映射
	h.proxyPortLock.Lock()
	h.proxyPortMap[req.RemotePort] = req.LocalPort
	h.proxyPortLock.Unlock()

	go h.StartProxyListener(req.RemotePort, clientID)

	c.JSON(http.StatusOK, model.Success(gin.H{
		"public_addr": "your-server.com:" + strconv.Itoa(req.RemotePort),
	}))
}

// GET /api/v1/clients/:id/ports
// ListClientPorts 获取指定客户端的端口列表
func (h *APIHandler) ListClientPorts(c *gin.Context) {
	clientID := c.Param("id")

	// 检查客户端是否在线
	conn, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	// 生成请求 ID
	requestID := uuid.New().String()

	// 创建响应通道（用于等待 Client 回复）
	respChan := make(chan *model.PortsListData, 1)
	h.clientMgr.RegisterPendingRequest(requestID, respChan)
	defer h.clientMgr.UnregisterPendingRequest(requestID)

	// 构造并发送命令
	cmd := model.WSMessage{
		Type: "list_ports",
		Data: model.ListPortsCmd{RequestID: requestID},
	}
	if err := conn.WriteJSON(cmd); err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send command: "+err.Error()))
		return
	}

	// 等待响应（带超时）
	select {
	case resp := <-respChan:
		if resp.Error != "" {
			c.JSON(http.StatusInternalServerError, model.Error(500, resp.Error))
			return
		}
		c.JSON(http.StatusOK, model.Success(gin.H{
			"client_id": clientID,
			"ports":     resp.Ports,
		}))
	case <-time.After(10 * time.Second):
		c.JSON(http.StatusGatewayTimeout, model.Error(504, "client response timeout"))
	}
}
