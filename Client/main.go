// Package main 是 Aether 客户端入口
//
// 客户端连接到 Aether 服务端，注册身份并建立隧道。
// 支持 TCP 和 WebSocket 两种隧道模式，自动重连。
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// 全局配置变量
var (
	serverURL      = os.Getenv("AETHER_WS_URL") // 服务端 WebSocket 地址
	reconnectDelay = 5 * time.Second            // 重连延迟
	useHTTP        bool                         // 是否使用 HTTP 模式
)

func init() {
	if serverURL == "" {
		serverURL = "wss://your-server.com:9909/ws"
	}
}

func main() {
	// 解析命令行参数
	var clientID string
	flag.StringVar(&clientID, "id", "raspberry-pi-01", "Client ID for identification")
	flag.BoolVar(&useHTTP, "http", false, "Use HTTP/WS instead of HTTPS/WSS")
	flag.Parse()

	// HTTP 模式下切换协议
	if useHTTP {
		serverURL = strings.Replace(serverURL, "wss://", "ws://", 1)
	}

	// 验证客户端令牌
	clientToken := os.Getenv("AETHER_CLIENT_TOKEN")
	if clientToken == "" {
		log.Fatal("AETHER_CLIENT_TOKEN environment variable is required")
	}

	// 创建客户端
	client := NewClient(serverURL, clientID, clientToken)

	// 注册信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		client.Stop()
	}()

	// 启动客户端（自动重连）
	client.Run()
}
