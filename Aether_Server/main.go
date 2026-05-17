package main

import (
	"Aether/Aether_Server/handler"
	"Aether/Aether_Server/manager"
	"Aether/Aether_Server/middleware"
	"Aether/Aether_Server/register"
	"Aether/Aether_Server/storage"
	"Aether/common/config"
	alog "Aether/common/log"
	"Aether/common/model"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func printVersion() {
	fmt.Printf("Aether_Server %s (%s) %s\n", Version, GitCommit, BuildTime)
}

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
func getTunnelHost(cfg *config.ServerConfig, table *manager.ClientTable) string {
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
func restoreClientProxies(cfg *config.ServerConfig, clientMgr *manager.ClientManager, store *storage.Storage, h *handler.APIHandler) func(string, *manager.Connection) {
	return func(clientID string, conn *manager.Connection) {
		proxies := store.GetByClient(clientID)
		if len(proxies) == 0 {
			return
		}

		alog.Info(alog.CatClient, "客户端已连接，恢复代理配置", "clientID", clientID, "count", len(proxies))
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
				alog.Error(alog.CatProxy, "恢复代理失败", "clientID", clientID, "remotePort", p.RemotePort, "error", err)
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
				table.StoreTunnelToken(token, fmt.Sprintf("%s-%d", clientID, p.RemotePort))
				go h.StartTCPProxy(p.RemotePort, p.BindAddr, table, token)
			}

			alog.Info(alog.CatProxy, "已恢复代理", "protocol", p.Protocol, "remotePort", p.RemotePort, "localIP", p.LocalIP, "localPort", p.LocalPort)
		}
	}
}

func main() {
	// 定义命令行参数
	configPath := flag.String("config", "config.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	// 解析命令行参数
	flag.Parse()

	if *showVersion {
		printVersion()
		return
	}

	// 加载配置文件
	cfg, err := config.LoadServer(*configPath)
	if err != nil {
		alog.Fatal(alog.CatConfig, "加载配置失败", "error", err)
	}

	// 初始化日志文件
	if cfg.LogPath != "" {
		if err := alog.SetFile(cfg.LogPath); err != nil {
			alog.Fatal(alog.CatConfig, "初始化日志文件失败", "error", err, "path", cfg.LogPath)
		}
		alog.Info(alog.CatConfig, "日志文件已启用", "path", cfg.LogPath)
	}

	// 初始化中间件
	middleware.Init(cfg.DataDir)
	middleware.CleanupExpiredRegistrations()
	middleware.InitJWTSecret()

	// 初始化持久化存储
	store, err := storage.New(cfg.Storage)
	if err != nil {
		alog.Fatal(alog.CatConfig, "初始化存储失败", "error", err)
	}

	// 获取公网 IP
	publicIP := cfg.PublicIP
	if publicIP == "" {
		alog.Info(alog.CatSystem, "正在获取公网IP")
		publicIP = getPublicIP()
	}
	alog.Info(alog.CatSystem, "公网IP已确定", "publicIP", publicIP)

	// 初始化客户端管理器
	clientMgr := manager.NewClientManager(manager.Config{
		ClientToken: cfg.Auth.ClientToken,
		PublicIP:    publicIP,
	})

	// 初始化 CA
	caCertPath := filepath.Join(cfg.DataDir, "ca.crt")
	caKeyPath := filepath.Join(cfg.DataDir, "ca.key")
	err = register.InitCA(caCertPath, caKeyPath)
	if err != nil {
		alog.Fatal(alog.CatAuth, "初始化CA失败", "error", err)
	}
	alog.Info(alog.CatAuth, "CA初始化成功")

	// 初始化注册表
	registryPath := filepath.Join(cfg.DataDir, "registry.json")
	registry := register.NewRegistry(registryPath)

	// 设置客户端删除回调，断开已连接的客户端
	registry.SetOnClientDelete(func(clientID string) {
		if table, ok := clientMgr.Get(clientID); ok {
			alog.Warn(alog.CatAuth, "客户端证书被吊销，断开连接", "clientID", clientID)
			table.Conn().Close()
		}
	})

	registerHandler := handler.NewRegisterHandler(cfg, registry)

	// 初始化 API 处理器
	apiHandler := handler.NewAPIHandler(clientMgr, cfg.Server.Domain, cfg.Server.TunnelPort, store, cfg)

	// 初始化中继处理器
	relayHandler := handler.NewRelayHandler(clientMgr, cfg.Server.Domain)

	// 设置客户端注册回调
	clientMgr.SetOnClientReady(restoreClientProxies(cfg, clientMgr, store, apiHandler))

	// 设置中继消息回调
	clientMgr.SetOnRelayMessage(func(clientID string, msg interface{}) {
		if wsMsg, ok := msg.(*model.WSMessage); ok {
			relayHandler.HandleClientStatus(wsMsg, clientID)
		}
	})

	// 初始化 Gin 路由
	r := gin.Default()
	r.Use(gin.Recovery(), gin.Logger())

	// 健康检查端点
	r.GET("/PING", func(context *gin.Context) {
		context.JSON(http.StatusOK, model.Success(gin.H{
			"message": "PANG",
		}))
	})

	login := r.Group("/api/v1")
	login.Use(middleware.RateLimit(10))
	{
		login.POST("/login", apiHandler.HandleLogin)
	}

	// 公开端点（不需要认证）
	public := r.Group("/api/v1")
	public.Use(middleware.RateLimit(60))
	{
		public.POST("/register_apply", registerHandler.HandleRegisterApply)
		public.GET("/register_info", registerHandler.HandleRegisterInfo)
	}

	// 需要认证的端点（使用 JWT Token 认证）
	api := r.Group("/api/v1")
	api.Use(middleware.JWTAuth())
	api.Use(middleware.RateLimit(60))
	{
		api.POST("/register_add", registerHandler.HandleRegisterAdd)
		api.POST("/register_delete", registerHandler.HandleRegisterDelete)
		api.GET("/register_apply_list", registerHandler.HandleRegisterApplyList)
		api.GET("/clients", apiHandler.ListClients)                  // 列出所有已连接客户端
		api.GET("/clients/:id/info", apiHandler.GetClientInfo)       // 获取客户端代理信息
		api.POST("/clients/:id/proxy", apiHandler.CreateProxy)       // 创建代理映射
		api.GET("/proxies", apiHandler.ListProxies)                  // 列出所有代理
		api.DELETE("/proxies/:port", apiHandler.CloseProxy)          // 关闭代理
		api.POST("/relay/connect", relayHandler.CreateRelay)         // 创建中继连接
		api.GET("/relay/sessions", relayHandler.ListSessions)        // 列出中继会话
		api.DELETE("/relay/sessions/:id", relayHandler.CloseSession) // 关闭中继会话
		api.POST("/update", apiHandler.HandleUpdateServer)              // 更新服务端二进制并重启
		api.POST("/clients/:id/update", apiHandler.HandleClientUpdate) // 更新指定客户端
	}

	// WebSocket 端点
	wsHandler := handler.NewWSHandler(clientMgr, registry, cfg.Server.Domain)
	r.GET("/ws", wsHandler.Handle)              // 客户端注册连接
	r.GET("/tunnel", wsHandler.HandleTunnelWS)  // WebSocket 隧道
	r.GET("/relay", relayHandler.HandleRelayWS) // 中继 WebSocket

	// 启动隧道监听器
	if cfg.Server.TunnelPort > 0 {
		tunnelListener, err := handler.NewTunnelListener(cfg.Server.TunnelPort, clientMgr)
		if err != nil {
			alog.Fatal(alog.CatTunnel, "初始化隧道监听器失败", "error", err)
		}
		defer tunnelListener.Close()
		go tunnelListener.Start()
		alog.Info(alog.CatTunnel, "隧道监听器已启动", "port", cfg.Server.TunnelPort)
	}

	// 打印已保存的代理配置
	proxies := store.GetAll()
	if len(proxies) > 0 {
		alog.Info(alog.CatProxy, "已加载代理配置，等待客户端连接后自动恢复", "count", len(proxies))
		for _, p := range proxies {
			alog.Info(alog.CatProxy, "代理配置详情", "clientID", p.ClientID, "protocol", p.Protocol, "remotePort", p.RemotePort, "localIP", p.LocalIP, "localPort", p.LocalPort)
		}
	}

	// 启动 HTTP/HTTPS 服务
	caCert, _ := register.GetCA()
	if caCert == nil {
		alog.Fatal(alog.CatAuth, "CA证书未加载")
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AddCert(caCert)

	server := &http.Server{
		Addr:    cfg.Server.Addr,
		Handler: r,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientCAs:  caCertPool,
			ClientAuth: tls.RequestClientCert, // 请求但不强制
		},
	}

	if cfg.TLS.Enabled {
		alog.Info(alog.CatServer, "正在启动HTTPS/WSS服务器（mTLS）", "addr", server.Addr)
		go func() {
			if err := server.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				alog.Fatal(alog.CatServer, "服务器错误", "error", err)
			}
		}()
	} else {
		alog.Info(alog.CatServer, "正在启动HTTP/WS服务器", "addr", server.Addr)
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				alog.Fatal(alog.CatServer, "服务器错误", "error", err)
			}
		}()
	}

	// 等待退出信号，优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	alog.Info(alog.CatServer, "收到信号，正在优雅关闭", "signal", sig)

	// 关闭 HTTP 服务器
	server.Close()

	// 关闭隧道监听器（defer 会执行）
	alog.Info(alog.CatServer, "服务已停止")
	alog.Flush()
}
