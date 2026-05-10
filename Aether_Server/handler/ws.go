package handler

import (
	"Aether/Aether_Server/manager"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// upgrader WebSocket 升级器配置
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许跨域连接，生产环境建议限制允许的 Origin
	},
	HandshakeTimeout: 10 * time.Second,
}

// WSHandler 处理 WebSocket 连接
type WSHandler struct {
	clientMgr *manager.ClientManager
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(mgr *manager.ClientManager) *WSHandler {
	return &WSHandler{clientMgr: mgr}
}

// Handle 处理客户端 WebSocket 注册连接
//
// 客户端通过此端点建立 WebSocket 连接并注册身份。
// 连接后会启动读写协程和心跳检测。
func (h *WSHandler) Handle(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}

	// 提取请求 Host
	host := c.Request.Host
	if hostHost, _, err := net.SplitHostPort(host); err == nil {
		host = hostHost
	}

	// 创建连接并启动
	clientConn := manager.NewConnection(conn, h.clientMgr)
	clientConn.SetHost(host)
	clientConn.Start()
}
