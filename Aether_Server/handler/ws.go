package handler

import (
	"Aether/Aether_Server/manager"
	"Aether/Aether_Server/register"
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
	registry  *register.Registry
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(mgr *manager.ClientManager, registry *register.Registry) *WSHandler {
	return &WSHandler{clientMgr: mgr, registry: registry}
}

// Handle 处理客户端 WebSocket 注册连接
func (h *WSHandler) Handle(c *gin.Context) {
	// 验证客户端证书
	certs := c.Request.TLS.PeerCertificates
	if len(certs) == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "client certificate required"})
		return
	}

	// 从证书中提取客户端 ID（CommonName）
	clientCert := certs[0]
	clientID := clientCert.Subject.CommonName

	// 验证证书是否在注册表中且状态为 approved
	record := h.registry.GetByClientID(clientID)
	if record == nil || record.Status != "approved" {
		log.Printf("客户端证书验证失败: %s (状态: %v)", clientID, record)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "client certificate revoked or not approved"})
		return
	}

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
