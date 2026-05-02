// Package main 是 Aether 隧道代理服务的入口
//
// Aether 是一个内网穿透工具，允许客户端将本地服务暴露到公网服务器。
// 支持 TCP、WebSocket 两种隧道模式，通过多路复用实现高效连接管理。
package main

import (
	"Aether/Server/config"
	"Aether/Server/handler"
	"Aether/Server/manager"
	"Aether/Server/middleware"
	"Aether/Server/model"
	"Aether/Server/storage"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// getPublicIP 自动获取服务器公网 IP
// 优先使用环境变量 AETHER_PUBLIC_IP，否则通过外部服务获取
func getPublicIP() string {
	addrs := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	for _, url := range addrs {
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		if net.ParseIP(ip) != nil {
			return ip
		}
	}

	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					ip := ipnet.IP
					if ip != nil && ip.To4() != nil && !ip.IsLoopback() {
						return ip.String()
					}
				}
			}
		}
	}

	return ""
}

// generateToken 生成随机令牌
func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// getTunnelHost 获取隧道主机地址
func getTunnelHost(cfg *config.Config, table *manager.ClientTable) string {
	serverHost := table.Host()
	if serverHost == "" {
		if cfg.Server.Domain != "" {
			serverHost = cfg.Server.Domain
		} else {
			serverHost = cfg.PublicIP
		}
	}
	return serverHost
}

// restoreClientProxies 恢复客户端代理配置
func restoreClientProxies(cfg *config.Config, clientMgr *manager.ClientManager, store *storage.Storage, h *handler.APIHandler) func(string, *manager.Connection) {
	return func(clientID string, conn *manager.Connection) {
		proxies := store.GetByClient(clientID)
		if len(proxies) == 0 {
			return
		}

		log.Printf("客户端 %s 已连接，恢复 %d 个代理配置", clientID, len(proxies))
		table, ok := clientMgr.Get(clientID)
		if !ok {
			return
		}

		for _, p := range proxies {
			token := generateToken()
			cmdData, _ := json.Marshal(model.CommandData{
				RemotePort: p.RemotePort,
				LocalPort:  p.LocalPort,
				Protocol:   p.Protocol,
				BindAddr:   p.BindAddr,
				ServerHost: getTunnelHost(cfg, table),
				TunnelPort: cfg.Server.TunnelPort,
				Token:      token,
				LocalIP:    p.LocalIP,
			})
			cmd := model.WSMessage{
				Type: "proxy",
				Data: string(cmdData),
			}

			if err := conn.WriteJSON(&cmd); err != nil {
				log.Printf("恢复代理失败 %s:%d: %v", clientID, p.RemotePort, err)
				continue
			}

			table.AddProxy(&manager.ProxyInfo{
				RemotePort: p.RemotePort,
				LocalPort:  p.LocalPort,
				LocalIP:    p.LocalIP,
				Protocol:   p.Protocol,
				BindAddr:   p.BindAddr,
			})
			clientMgr.RegisterPort(clientID, p.RemotePort)

			if p.Protocol == "websocket" {
				table.StoreWSToken(token, fmt.Sprintf("%s-%d", clientID, p.RemotePort))
				go h.StartWSProxy(p.RemotePort, p.BindAddr, table, token)
			} else {
				go h.StartTCPProxy(p.RemotePort, p.BindAddr, table, token)
			}

			log.Printf("  已恢复: %s %d -> %s:%d", p.Protocol, p.RemotePort, p.LocalIP, p.LocalPort)
		}
	}
}

func main() {
	// 解析命令行参数
	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	// 加载配置文件
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// 初始化持久化存储
	store, err := storage.New(cfg.Storage)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}

	// 获取公网 IP
	publicIP := cfg.PublicIP
	if publicIP == "" {
		log.Printf("Fetching public IP...")
		publicIP = getPublicIP()
	}
	log.Printf("Public IP: %s", publicIP)

	// 初始化客户端管理器
	clientMgr := manager.NewClientManager(manager.Config{
		ClientToken: cfg.Auth.ClientToken,
		PublicIP:    publicIP,
	})

	// 初始化 API 处理器
	apiHandler := handler.NewAPIHandler(clientMgr, cfg.Server.Domain, cfg.Server.TunnelPort, store)

	// 设置客户端注册回调
	clientMgr.SetOnClientReady(restoreClientProxies(cfg, clientMgr, store, apiHandler))

	// 初始化 Gin 路由
	r := gin.Default()
	r.Use(gin.Recovery(), gin.Logger())

	// 健康检查端点
	r.GET("/PING", func(context *gin.Context) {
		context.JSON(http.StatusOK, model.Success(gin.H{
			"message": "PANG",
		}))
	})

	// API 路由组（需要 API Key 认证）
	api := r.Group("/api/v1")
	api.Use(middleware.Auth(cfg.Auth.APIKey))
	{
		api.GET("/clients", apiHandler.ListClients)            // 列出所有已连接客户端
		api.GET("/clients/:id/info", apiHandler.GetClientInfo) // 获取客户端代理信息
		api.POST("/clients/:id/proxy", apiHandler.CreateProxy) // 创建代理映射
		api.GET("/proxies", apiHandler.ListProxies)            // 列出所有代理
		api.DELETE("/proxies/:port", apiHandler.CloseProxy)    // 关闭代理
	}

	// WebSocket 端点
	wsHandler := handler.NewWSHandler(clientMgr)
	r.GET("/ws", wsHandler.Handle)             // 客户端注册连接
	r.GET("/tunnel", wsHandler.HandleTunnelWS) // WebSocket 隧道

	// 启动隧道监听器
	if cfg.Server.TunnelPort > 0 {
		tunnelListener, err := handler.NewTunnelListener(cfg.Server.TunnelPort, clientMgr)
		if err != nil {
			log.Fatalf("init tunnel listener: %v", err)
		}
		defer tunnelListener.Close()
		go tunnelListener.Start()
		log.Printf("Tunnel listener started on :%d", cfg.Server.TunnelPort)
	}

	// 打印已保存的代理配置
	proxies := store.GetAll()
	if len(proxies) > 0 {
		log.Printf("已加载 %d 个代理配置，等待客户端连接后自动恢复", len(proxies))
		for _, p := range proxies {
			log.Printf("  - %s: %s %d -> %s:%d", p.ClientID, p.Protocol, p.RemotePort, p.LocalIP, p.LocalPort)
		}
	}

	// 启动 HTTP/HTTPS 服务
	server := &http.Server{
		Addr:    cfg.Server.Addr,
		Handler: r,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	if cfg.TLS.Enabled {
		log.Printf("Starting HTTPS/WSS server on %s", server.Addr)
		log.Fatal(server.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile))
	} else {
		log.Printf("Starting HTTP/WS server on %s", server.Addr)
		log.Fatal(server.ListenAndServe())
	}
}
