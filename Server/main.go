package main

import (
	"Aether/Server/handler"
	"Aether/Server/manager"
	"Aether/Server/middleware"
	"Aether/Server/model"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	apiKey      = "X-API-KEY"
	clientToken = "your-client-token"
)

func main() {
	clientMgr := manager.NewClientManager(manager.Config{ClientToken: clientToken})
	tunnelMgr := manager.NewTunnelManager() // 隧道管理器

	r := gin.Default()
	r.Use(gin.Recovery(), gin.Logger())

	r.GET("/PING", func(context *gin.Context) {
		context.JSON(http.StatusOK, model.Success(gin.H{
			"message": "PANG",
		}))
	})

	api := r.Group("/api/v1")
	api.Use(middleware.Auth(apiKey))
	{
		h := handler.NewAPIHandler(clientMgr, tunnelMgr) // 传入 tunnelMgr
		api.GET("/clients", h.ListClients)
		api.GET("/clients/:id/ports", h.ListClientPorts)
		api.POST("/clients/:id/proxy", h.CreateProxy)
		api.DELETE("/proxies/:port", h.CloseProxy)
	}

	// WebSocket 路由
	wsHandler := handler.NewWSHandler(clientMgr, tunnelMgr) // 传入 tunnelMgr
	r.GET("/ws", wsHandler.Handle)

	log.Fatal(r.Run(":9909"))
}
