package handler

import (
	"Aether/Server/manager"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 开发环境允许所有来源，生产应限制
	},
	HandshakeTimeout: 10 * time.Second,
}

type WSHandler struct {
	clientMgr *manager.ClientManager
	tunnelMgr *manager.TunnelManager
}

func NewWSHandler(mgr *manager.ClientManager, tunnelMgr *manager.TunnelManager) *WSHandler {
	return &WSHandler{clientMgr: mgr, tunnelMgr: tunnelMgr}
}

func (h *WSHandler) Handle(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}

	// 创建连接对象并启动
	clientConn := manager.NewConnection(conn, h.clientMgr, h.tunnelMgr)
	clientConn.Start()

	// 启动一个定时器，如果 30 秒内未完成注册则断开
	go func() {
		time.Sleep(30 * time.Second)
		if !clientConn.IsRegistered() {
			log.Println("client registration timeout, closing connection")
			clientConn.Close()
		}
	}()
}
