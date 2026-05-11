package handler

import (
	"Aether/Aether_Server/manager"
	"Aether/Aether_Server/register"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func newUpgrader(domain string) *websocket.Upgrader {
	return &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true
			}
			if domain == "" {
				return true
			}
			allowed := fmt.Sprintf("https://%s", domain)
			if strings.HasPrefix(origin, allowed) {
				return true
			}
			log.Printf("WebSocket 连接被拒绝: origin=%s", origin)
			return false
		},
		HandshakeTimeout: 10 * time.Second,
	}
}

// WSHandler 处理 WebSocket 连接
type WSHandler struct {
	clientMgr *manager.ClientManager
	registry  *register.Registry
	domain    string
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(mgr *manager.ClientManager, registry *register.Registry, domain string) *WSHandler {
	return &WSHandler{clientMgr: mgr, registry: registry, domain: domain}
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

	conn, err := newUpgrader(h.domain).Upgrade(c.Writer, c.Request, nil)
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
