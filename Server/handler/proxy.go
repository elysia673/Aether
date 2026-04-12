package handler

import (
	"Aether/Server/manager"
	"Aether/Server/model"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (h *APIHandler) StartProxyListener(port int, clientID string) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("failed to listen on port %d: %v", port, err)
		return
	}
	defer ln.Close()

	h.listenerLock.Lock()
	h.proxyListeners[port] = ln
	h.listenerLock.Unlock()

	log.Printf("proxy listening on :%d for client %s", port, clientID)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error on port %d: %v", port, err)
			continue
		}
		go h.handleProxyConn(conn, clientID, port)
	}
}

func (h *APIHandler) handleProxyConn(conn net.Conn, clientID string, remotePort int) {
	h.proxyPortLock.RLock()
	localPort, ok := h.proxyPortMap[remotePort]
	h.proxyPortLock.RUnlock()
	if !ok {
		log.Printf("no local port mapping for remote port %d", remotePort)
		conn.Close()
		return
	}

	tunnelID := uuid.New().String()
	tunnel := &manager.Tunnel{
		ID:         tunnelID,
		ClientID:   clientID,
		RemotePort: remotePort,
		LocalPort:  localPort,
		Conn:       conn,
		CreatedAt:  time.Now().Unix(),
	}
	h.tunnelMgr.Add(tunnelID, tunnel)

	cmd := model.WSMessage{
		Type: "new_tunnel",
		Data: map[string]interface{}{
			"tunnel_id":  tunnelID,
			"local_port": localPort,
		},
	}
	if err := h.clientMgr.SendCommand(clientID, cmd); err != nil {
		log.Printf("failed to send new_tunnel to client %s: %v", clientID, err)
		conn.Close()
		h.tunnelMgr.Remove(tunnelID)
		return
	}

	// 启动从公网连接读取并转发到客户端的 goroutine
	go h.tunnelMgr.ForwardToClient(tunnelID, h.clientMgr) // 需调整参数
}

// DELETE /api/v1/proxies/:port
func (h *APIHandler) CloseProxy(c *gin.Context) {
	portStr := c.Param("port")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid port"))
		return
	}

	h.listenerLock.Lock()
	ln, ok := h.proxyListeners[port]
	if ok {
		ln.Close()
		delete(h.proxyListeners, port)
	}
	h.listenerLock.Unlock()

	h.proxyPortLock.Lock()
	delete(h.proxyPortMap, port)
	h.proxyPortLock.Unlock()

	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "proxy not found"))
		return
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message": "proxy closed",
	}))
}
