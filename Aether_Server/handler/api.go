// Package handler 提供 HTTP/WebSocket 请求处理
//
// 包含 API 接口、WebSocket 连接管理和代理隧道处理。
package handler

import (
	"Aether/Aether_Server/manager"
	"Aether/Aether_Server/storage"
	"Aether/common/model"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// APIHandler 处理 REST API 请求
type APIHandler struct {
	clientMgr  *manager.ClientManager // 客户端管理器
	domain     string                 // 公网域名
	tunnelPort int                    // 隧道端口
	store      *storage.Storage       // 持久化存储
}

// NewAPIHandler 创建 API 处理器
func NewAPIHandler(clientMgr *manager.ClientManager, domain string, tunnelPort int, store *storage.Storage) *APIHandler {
	return &APIHandler{
		clientMgr:  clientMgr,
		domain:     domain,
		tunnelPort: tunnelPort,
		store:      store,
	}
}

// ListClients 返回所有已连接的客户端列表
func (h *APIHandler) ListClients(c *gin.Context) {
	clients := h.clientMgr.ListClients()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"clients": clients,
	}))
}

// proxyRequest 创建代理的请求参数
type proxyRequest struct {
	RemotePort int    `json:"remote_port" binding:"required,min=1,max=65535"`                 // 服务端暴露端口
	LocalPort  int    `json:"local_port" binding:"required,min=1,max=65535"`                  // 客户端本地端口
	Protocol   string `json:"protocol" binding:"required,oneof=tcp udp http https websocket"` // 协议类型
	BindAddr   string `json:"bind_addr"`                                                      // 服务端绑定地址
	LocalIP    string `json:"local_ip"`                                                       // 客户端本地 IP
}

// CreateProxy 创建代理映射
//
// 流程：
// 1. 验证请求参数
// 2. 生成隧道认证 token
// 3. 向客户端发送代理命令
// 4. 启动服务端代理监听
// 5. 返回公网访问地址
func (h *APIHandler) CreateProxy(c *gin.Context) {
	clientID := c.Param("id")
	var req proxyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	tunnelHost := table.TunnelHost(h.clientMgr.GetPublicIP())

	token := generateToken()

	cmdData, err := json.Marshal(model.CommandData{
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
		ServerHost: tunnelHost,
		TunnelPort: h.tunnelPort,
		Token:      token,
		LocalIP:    req.LocalIP,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to marshal command"))
		return
	}
	cmd := model.WSMessage{
		Type: "proxy",
		Data: string(cmdData),
	}

	if err := h.clientMgr.SendCommand(clientID, cmd); err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send command: "+err.Error()))
		return
	}

	table.AddProxy(&manager.ProxyInfo{
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		LocalIP:    req.LocalIP,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
	})
	h.clientMgr.RegisterPort(clientID, req.RemotePort)

	// 持久化保存
	h.store.Add(storage.ProxyRecord{
		ClientID:   clientID,
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		LocalIP:    req.LocalIP,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
	})

	if req.Protocol == "websocket" {
		table.StoreWSToken(token, fmt.Sprintf("%s-%d", clientID, req.RemotePort))
		go h.StartWSProxy(req.RemotePort, req.BindAddr, table, token)
	} else {
		// 存储隧道 token（用于隧道端口认证）
		table.StoreTunnelToken(token, fmt.Sprintf("%s-%d", clientID, req.RemotePort))
		go h.StartTCPProxy(req.RemotePort, req.BindAddr, table, token)
	}

	publicAddr := h.domain
	if publicAddr == "" {
		publicAddr = h.clientMgr.GetPublicIP()
	}
	c.JSON(http.StatusOK, model.Success(gin.H{
		"public_addr": publicAddr + ":" + strconv.Itoa(req.RemotePort),
	}))
}

// generateToken 生成随机令牌。
func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func (h *APIHandler) GetClientInfo(c *gin.Context) {
	clientID := c.Param("id")

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	proxies := table.ListProxies()
	ports := make([]gin.H, 0, len(proxies))
	for _, p := range proxies {
		ports = append(ports, gin.H{
			"remote_port": p.RemotePort,
			"local_port":  p.LocalPort,
			"local_ip":    p.LocalIP,
			"protocol":    p.Protocol,
			"bind_addr":   p.BindAddr,
		})
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"client_id": clientID,
		"ports":     ports,
	}))
}
