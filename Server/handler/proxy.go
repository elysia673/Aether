package handler

import (
	"Aether/Server/model"
	"net/http"
	"strconv"

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

	ln := table.GetProxyListener(port)
	if ln == nil {
		c.JSON(http.StatusNotFound, model.Error(404, "proxy not found"))
		return
	}

	key := table.TunnelKey(port)
	notifyMsg := model.WSMessage{
		Type: "proxy_closed",
		Data: map[string]string{"key": key},
	}
	table.Conn().WriteJSON(&notifyMsg)

	if err := ln.Close(); err != nil {
	}

	table.RemoveProxy(port)
	table.RemoveTunnelTokenByKey(key)
	h.clientMgr.UnregisterPort(port)

	// 从持久化存储中删除
	h.store.Remove(clientID, port)

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message": "proxy closed",
	}))
}

func (h *APIHandler) ListProxies(c *gin.Context) {
	proxies := h.clientMgr.ListAllProxies()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"proxies": proxies,
	}))
}
