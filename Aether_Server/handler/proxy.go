package handler

import (
	"Aether/Aether_Server/manager"
	alog "Aether/common/log"
	"Aether/common/model"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *APIHandler) CloseProxy(c *gin.Context) {
	portStr := c.Param("port")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid port"))
		return
	}

	clientID, ok := h.clientMgr.GetClientIDByPort(port)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "proxy not found"))
		return
	}

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	key := table.TunnelKey(port)

	// 注册待确认通道
	ackCh := make(chan struct{}, 1)
	h.clientMgr.RegisterPendingClose(key, ackCh)
	defer h.clientMgr.UnregisterPendingClose(key)

	// 发送关闭请求给客户端
	notifyMsg := model.WSMessage{
		Type: "proxy_closed",
		Data: map[string]string{"key": key},
	}
	if err := table.Conn().WriteJSON(&notifyMsg); err != nil {
		alog.Error(alog.CatProxy, "send proxy_closed failed", "error", err)
		// 客户端已断开，直接清理
		h.cleanupProxy(table, port, key, clientID)
		c.JSON(http.StatusOK, model.Success(gin.H{"message": "proxy closed"}))
		return
	}

	// 等待客户端确认（5秒超时）
	select {
	case <-ackCh:
		alog.Info(alog.CatProxy, "client acked proxy_closed", "key", key)
	case <-time.After(5 * time.Second):
		alog.Warn(alog.CatProxy, "client ack timeout", "key", key)
	}

	// 清理状态
	h.cleanupProxy(table, port, key, clientID)

	c.JSON(http.StatusOK, model.Success(gin.H{"message": "proxy closed"}))
}

func (h *APIHandler) cleanupProxy(table *manager.ClientTable, port int, key string, clientID string) {
	ln := table.GetProxyListener(port)
	table.RemoveProxy(port)
	table.RemoveTunnelTokenByKey(key)
	h.clientMgr.UnregisterPort(port)
	h.store.Remove(clientID, port)
	if ln != nil {
		ln.Close()
	}
}

func (h *APIHandler) ListProxies(c *gin.Context) {
	proxies := h.clientMgr.ListAllProxies()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"proxies": proxies,
	}))
}
