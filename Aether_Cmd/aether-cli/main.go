// Package main 是 Aether CLI 工具
//
// 用于管理 Aether 服务端，支持所有 API 操作。
// 默认配置文件：~/.aether_config.json
package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// CLIConfig CLI 配置
type CLIConfig struct {
	Server   string `json:"server"`  // 服务端地址，如 https://your-server.com:9909
	APIKey   string `json:"api_key"` // API 访问密钥
	Token    string `json:"token"`
	TokenExp int64  `json:"token_exp"`
	Insecure bool   `json:"insecure"` // 跳过 TLS 验证
}

// Response API 响应
type Response struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data,omitempty"`
}

var (
	configPath  string
	jsonOutput  bool
	showVersion bool
	insecure    bool
	cfg         *CLIConfig
	httpClient  *http.Client
)

func printVersion() {
	fmt.Printf("Aether Client %s (%s) %s\n", Version, GitCommit, BuildTime)
}

func init() {
	home := getHomeDir()
	defaultConfig := filepath.Join(home, ".aether_config.json")

	flag.StringVar(&configPath, "config", defaultConfig, "配置文件路径")
	flag.BoolVar(&jsonOutput, "json", false, "JSON 输出模式")
	flag.BoolVar(&showVersion, "version", false, "打印版本")
	flag.BoolVar(&insecure, "insecure", false, "跳过 TLS 验证（自签名证书）")

	// 自定义帮助信息
	flag.Usage = printUsage
}

func main() {

	flag.Parse()

	if showVersion {
		printVersion()
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
	}

	// 加载配置
	var err error
	cfg, err = loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}

	// 初始化持久化 HTTP 客户端（连接复用，避免每次 TLS 握手）
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		Resolver: &net.Resolver{
			PreferGo:     true,
			StrictErrors: false,
		},
	}
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: cfg.Insecure || insecure},
		DialContext:           dialer.DialContext,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	httpClient = &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	// 分发命令
	command := args[0]
	switch command {
	case "login":
		cmdLogin(args[1:]...)
	case "register":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli register <apply|add|delete|apply_list|info>\n")
			os.Exit(1)
		}
		switch args[1] {
		case "apply":
			cmdRegisterApply(args[2:]...)
		case "add":
			cmdRegisterAdd(args[2:]...)
		case "delete":
			cmdRegisterDelete(args[2:]...)
		case "apply_list":
			cmdRegisterApplyList()
		case "info":
			cmdRegisterInfo()
		default:
			fmt.Fprintf(os.Stderr, "未知子命令: %s\n", args[1])
			os.Exit(1)
		}
	case "ping":
		cmdPing()
	case "clients":
		cmdListClients()
	case "info":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli info <client-id>\n")
			os.Exit(1)
		}
		cmdClientInfo(args[1])
	case "proxies":
		cmdListProxies()
	case "create":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli create <client-id> [options]\n")
			os.Exit(1)
		}
		cmdCreateProxy(args[1], args[2:])
	case "close":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli close <port>\n")
			os.Exit(1)
		}
		cmdCloseProxy(args[1])
	case "relay":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli relay <落地端A> <服务端B> [options]\n")
			os.Exit(1)
		}
		cmdRelayConnect(args[1], args[2], args[3:])
	case "relay-sessions":
		cmdRelaySessions()
	case "relay-close":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli relay-close <session-id>\n")
			os.Exit(1)
		}
		cmdRelayClose(args[1])
	case "update":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "用法: aether-cli update <server|client> <binary-path> [options]\n")
			os.Exit(1)
		}
		switch args[1] {
		case "server":
			if len(args) < 3 {
				fmt.Fprintf(os.Stderr, "用法: aether-cli update server <binary-path>\n")
				os.Exit(1)
			}
			cmdUpdateServer(args[2])
		case "client":
			cmdUpdateClient(args[2:])
		default:
			fmt.Fprintf(os.Stderr, "未知子命令: %s\n", args[1])
			fmt.Fprintf(os.Stderr, "用法: aether-cli update <server|client> <binary-path> [options]\n")
			os.Exit(1)
		}
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`Aether CLI - 隧道代理管理工具

用法:
  aether-cli [flags] <command> [args...]

命令:
  login                         登录获取 JWT Token
  register apply                提交注册申请
  register add                  审核通过并签发证书
  register delete               吊销客户端证书
  register apply_list           查看待审核列表
  register info                 查看已通过列表
  ping                          健康检查
  clients                       列出所有客户端
  info <client-id>              获取客户端代理信息
  proxies                       列出所有代理
  create <client-id> [options]  创建代理映射
  close <port>                  关闭代理
  relay <落地端A> <服务端B> [options]  A监听端口 → B的服务端口
  relay-sessions                列出中继会话
  relay-close <session-id>      关闭中继会话
  update server <path>          更新服务端二进制
  update client <path>          更新客户端二进制
  help                          显示帮助

注册申请选项:
  -id <client-id>       客户端 ID (必填)
  -pubkey <path>        公钥文件路径 (必填)
  -token <token>        认证 token (必填)

审核通过选项:
  -id <client-id>       客户端 ID (必填)

吊销客户端选项:
  -id <client-id>       客户端 ID (必填)
  -prefix <prefix>      证书前40字符 (必填，用于二次确认)

create 选项:
  -remote <port>        服务端暴露端口 (必填)
  -local <port>         客户端本地端口 (必填)
  -protocol <type>      协议类型: tcp, udp, websocket (默认 tcp)
  -bind <addr>          服务端绑定地址 (默认 0.0.0.0)
  -local-ip <ip>        客户端本地 IP (默认 127.0.0.1)

relay 选项:
  -source-port <port>   源端本地监听端口 (必填)
  -target-port <port>   目标端本地服务端口 (必填)
  -protocol <type>      协议类型: tcp, udp, websocket (默认 tcp)
  -target-ip <ip>       目标端本地 IP (默认 127.0.0.1)
  -source-ip <ip>       源端监听 IP (默认 0.0.0.0)
  -source-peer <addr>   源端直连对端地址 (ip:port)，同 LAN 时指定内网 IP
  -target-peer <addr>   目标端直连对端地址 (ip:port)，同 LAN 时指定内网 IP

update 选项:
  -f <path>             二进制文件路径 (必填)
  -target <id>          目标客户端 ID 或 all (client 子命令必填)

全局选项:
  -config <path>      配置文件路径 (默认 ~/.aether_config.json)
  -json               JSON 输出模式
  -version            版本
`)
	os.Exit(0)
}

func getHomeDir() string {
	usr, err := user.Current()
	if err != nil {
		return "."
	}
	return usr.HomeDir
}

func loadConfig(path string) (*CLIConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &CLIConfig{}, nil
		}
		return nil, fmt.Errorf("读取配置文件: %w", err)
	}

	var cfg CLIConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件: %w", err)
	}

	// 确保 server 地址格式正确
	cfg.Server = strings.TrimRight(cfg.Server, "/")

	return &cfg, nil
}

func apiRequest(method, path string, body interface{}) (*Response, error) {
	url := cfg.Server + path

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("序列化请求: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("创建请求: %w", err)
	}

	// 使用 JWT Token 认证
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	} else {
		req.Header.Set("X-API-KEY", cfg.APIKey)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应: %w", err)
	}

	// 检查 HTTP 状态码
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	// 检查响应是否为空
	if len(respBody) == 0 {
		return nil, fmt.Errorf("空响应 (HTTP %d)", resp.StatusCode)
	}

	var result Response
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("解析响应: %w\n原始响应: %s", err, string(respBody))
	}

	return &result, nil
}

func printJSON(data interface{}) {
	output, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "JSON 序列化失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

func printResponse(resp *Response) {
	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	fmt.Printf("成功: %s\n", resp.Msg)
	if len(resp.Data) > 0 {
		var data interface{}
		json.Unmarshal(resp.Data, &data)
		printData(data)
	}
}

func printData(data interface{}) {
	switch v := data.(type) {
	case map[string]interface{}:
		printMap(v)
	case []interface{}:
		printList(v)
	default:
		fmt.Println(v)
	}
}

func printMap(m map[string]interface{}) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for k, v := range m {
		switch val := v.(type) {
		case []interface{}:
			fmt.Fprintf(w, "%s:\n", k)
			for _, item := range val {
				if m, ok := item.(map[string]interface{}); ok {
					fmt.Fprintf(w, "  -\n")
					for k2, v2 := range m {
						fmt.Fprintf(w, "    %s:\t%v\n", k2, v2)
					}
				} else {
					fmt.Fprintf(w, "  - %v\n", item)
				}
			}
		default:
			fmt.Fprintf(w, "%s:\t%v\n", k, v)
		}
	}
	w.Flush()
}

func printList(list []interface{}) {
	for _, item := range list {
		if m, ok := item.(map[string]interface{}); ok {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			for k, v := range m {
				fmt.Fprintf(w, "%s:\t%v\n", k, v)
			}
			w.Flush()
			fmt.Println("---")
		} else {
			fmt.Printf("- %v\n", item)
		}
	}
}

// ============ 命令实现 ============

func cmdPing() {
	resp, err := apiRequest("GET", "/PING", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code == 0 {
		var data map[string]interface{}
		json.Unmarshal(resp.Data, &data)
		if msg, ok := data["message"]; ok {
			fmt.Println(msg)
		}
	} else {
		fmt.Fprintf(os.Stderr, "错误: %s\n", resp.Msg)
		os.Exit(1)
	}
}

func cmdListClients() {
	resp, err := apiRequest("GET", "/api/v1/clients", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Clients []struct {
			ID          string `json:"id"`
			RemoteAddr  string `json:"remote_addr"`
			ConnectedAt int64  `json:"connected_at"`
			ProxyCount  int    `json:"proxy_count"`
			Host        string `json:"host"`
		} `json:"clients"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Clients) == 0 {
		fmt.Println("没有已连接的客户端")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "ID\t地址\t代理数\t主机\n")
	fmt.Fprintf(w, "--\t----\t------\t----\n")
	for _, c := range data.Clients {
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", c.ID, c.RemoteAddr, c.ProxyCount, c.Host)
	}
	w.Flush()
}

func cmdClientInfo(clientID string) {
	resp, err := apiRequest("GET", fmt.Sprintf("/api/v1/clients/%s/info", clientID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		ClientID string `json:"client_id"`
		Ports    []struct {
			RemotePort int    `json:"remote_port"`
			LocalPort  int    `json:"local_port"`
			LocalIP    string `json:"local_ip"`
			Protocol   string `json:"protocol"`
			BindAddr   string `json:"bind_addr"`
		} `json:"ports"`
	}
	json.Unmarshal(resp.Data, &data)

	fmt.Printf("客户端: %s\n", data.ClientID)
	if len(data.Ports) == 0 {
		fmt.Println("没有代理映射")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "远程端口\t本地地址\t协议\t绑定地址\n")
	fmt.Fprintf(w, "--------\t--------\t----\t--------\n")
	for _, p := range data.Ports {
		fmt.Fprintf(w, "%d\t%s:%d\t%s\t%s\n", p.RemotePort, p.LocalIP, p.LocalPort, p.Protocol, p.BindAddr)
	}
	w.Flush()
}

func cmdListProxies() {
	resp, err := apiRequest("GET", "/api/v1/proxies", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Proxies []struct {
			RemotePort int    `json:"remote_port"`
			LocalPort  int    `json:"local_port"`
			PublicAddr string `json:"public_addr"`
			ClientID   string `json:"client_id"`
		} `json:"proxies"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Proxies) == 0 {
		fmt.Println("没有活跃的代理")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "客户端\t远程端口\t本地端口\t公网地址\n")
	fmt.Fprintf(w, "------\t--------\t--------\t--------\n")
	for _, p := range data.Proxies {
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\n", p.ClientID, p.RemotePort, p.LocalPort, p.PublicAddr)
	}
	w.Flush()
}

func cmdCreateProxy(clientID string, args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)

	remotePort := fs.Int("remote", 0, "服务端暴露端口")
	localPort := fs.Int("local", 0, "客户端本地端口")
	protocol := fs.String("protocol", "tcp", "协议类型: tcp, udp, websocket")
	bindAddr := fs.String("bind", "0.0.0.0", "服务端绑定地址")
	localIP := fs.String("local-ip", "127.0.0.1", "客户端本地 IP")

	fs.Parse(args)

	if *remotePort == 0 || *localPort == 0 {
		fmt.Fprintf(os.Stderr, "错误: -remote 和 -local 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	body := map[string]interface{}{
		"remote_port": *remotePort,
		"local_port":  *localPort,
		"protocol":    *protocol,
		"bind_addr":   *bindAddr,
		"local_ip":    *localIP,
	}

	resp, err := apiRequest("POST", fmt.Sprintf("/api/v1/clients/%s/proxy", clientID), body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		PublicAddr string `json:"public_addr"`
	}
	json.Unmarshal(resp.Data, &data)

	fmt.Printf("代理创建成功\n")
	fmt.Printf("公网地址: %s\n", data.PublicAddr)
}

func cmdCloseProxy(portStr string) {
	resp, err := apiRequest("DELETE", fmt.Sprintf("/api/v1/proxies/%s", portStr), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	fmt.Printf("代理已关闭 (端口 %s)\n", portStr)
}

func cmdRelayConnect(sourceID, targetID string, args []string) {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)

	sourcePort := fs.Int("source-port", 0, "源端本地监听端口")
	targetPort := fs.Int("target-port", 0, "目标端本地服务端口")
	protocol := fs.String("protocol", "tcp", "协议类型: tcp, udp, websocket")
	targetIP := fs.String("target-ip", "127.0.0.1", "目标端本地 IP")
	sourceIP := fs.String("source-ip", "0.0.0.0", "源端监听 IP")
	sourcePeer := fs.String("source-peer", "", "源端直连对端地址 (ip:port)，同 LAN 时指定内网 IP")
	targetPeer := fs.String("target-peer", "", "目标端直连对端地址 (ip:port)，同 LAN 时指定内网 IP")

	fs.Parse(args)

	if *sourcePort == 0 || *targetPort == 0 {
		fmt.Fprintf(os.Stderr, "错误: -source-port 和 -target-port 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	body := map[string]interface{}{
		"source_client_id": sourceID,
		"target_client_id": targetID,
		"target_port":      *targetPort,
		"source_port":      *sourcePort,
		"protocol":         *protocol,
		"target_local_ip":  *targetIP,
		"source_local_ip":  *sourceIP,
	}
	if *sourcePeer != "" {
		body["source_peer_addr"] = *sourcePeer
	}
	if *targetPeer != "" {
		body["target_peer_addr"] = *targetPeer
	}

	resp, err := apiRequest("POST", "/api/v1/relay/connect", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		SessionID    string `json:"session_id"`
		SourceClient string `json:"source_client"`
		TargetClient string `json:"target_client"`
		Protocol     string `json:"protocol"`
	}
	json.Unmarshal(resp.Data, &data)

	fmt.Printf("中继连接已创建\n")
	fmt.Printf("会话 ID: %s\n", data.SessionID)
	fmt.Printf("源客户端: %s\n", data.SourceClient)
	fmt.Printf("目标客户端: %s\n", data.TargetClient)
	fmt.Printf("协议: %s\n", data.Protocol)
}

func cmdRelaySessions() {
	resp, err := apiRequest("GET", "/api/v1/relay/sessions", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Sessions []struct {
			SessionID    string `json:"session_id"`
			SourceClient string `json:"source_client"`
			TargetClient string `json:"target_client"`
			Protocol     string `json:"protocol"`
			SourcePort   int    `json:"source_port"`
			TargetPort   int    `json:"target_port"`
			SourceReady  bool   `json:"source_ready"`
			TargetReady  bool   `json:"target_ready"`
			Status       string `json:"status"`
			Error        string `json:"error"`
			CreatedAt    int64  `json:"created_at"`
		} `json:"sessions"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Sessions) == 0 {
		fmt.Println("没有活跃的中继会话")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "会话 ID\t落地端A\t服务端B\t协议\t状态\t错误\n")
	fmt.Fprintf(w, "--------\t-------\t-------\t----\t----\t----\n")
	for _, s := range data.Sessions {
		showID := s.SessionID
		if len(showID) > 12 {
			showID = showID[:12]
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", showID, s.SourceClient, s.TargetClient, s.Protocol, s.Status, s.Error)
	}
	w.Flush()
}

func cmdRelayClose(sessionID string) {
	resp, err := apiRequest("DELETE", fmt.Sprintf("/api/v1/relay/sessions/%s", sessionID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}
	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}
	fmt.Printf("中继会话已关闭 (%s)\n", sessionID)
}

func cmdUpdateServer(binaryPath string) {
	md5sum := calcMD5(binaryPath)
	fmt.Printf("文件: %s\n", binaryPath)
	fmt.Printf("MD5:  %s\n", md5sum)
	fmt.Println("正在更新服务端...")
	uploadBinary(binaryPath, cfg.Server+"/api/v1/update", md5sum)
	fmt.Println("服务端更新成功，正在重启...")
}

func cmdUpdateClient(args []string) {
	fs := flag.NewFlagSet("update client", flag.ExitOnError)
	binaryPath := fs.String("f", "", "二进制文件路径 (必填)")
	target := fs.String("target", "", "目标客户端 ID 或 all (必填)")
	fs.Parse(args)

	if *binaryPath == "" || *target == "" {
		fmt.Fprintf(os.Stderr, "用法: aether-cli update client -f <binary-path> -target <client-id|all>\n")
		os.Exit(1)
	}

	md5sum := calcMD5(*binaryPath)
	fmt.Printf("文件: %s\n", *binaryPath)
	fmt.Printf("MD5:  %s\n", md5sum)

	if *target == "all" {
		updateAllClients(*binaryPath, md5sum)
	} else {
		updateClient(*binaryPath, md5sum, *target)
	}
}

func calcMD5(binaryPath string) string {
	file, err := os.Open(binaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开文件失败: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		fmt.Fprintf(os.Stderr, "计算 MD5 失败: %v\n", err)
		os.Exit(1)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func uploadBinary(binaryPath, url, md5sum string) {
	file, err := os.Open(binaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开文件失败: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("md5", md5sum)

	part, err := writer.CreateFormFile("binary", filepath.Base(binaryPath))
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建表单失败: %v\n", err)
		os.Exit(1)
	}
	if _, err := io.Copy(part, file); err != nil {
		fmt.Fprintf(os.Stderr, "读取文件失败: %v\n", err)
		os.Exit(1)
	}
	writer.Close()

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建请求失败: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	fmt.Println("正在上传...")
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "更新失败 (HTTP %d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(respBody, &result)

	if result.Code != 0 {
		fmt.Fprintf(os.Stderr, "更新失败: %s\n", result.Msg)
		os.Exit(1)
	}
}

func updateClient(binaryPath, md5sum, clientID string) {
	fmt.Printf("正在更新客户端 %s...\n", clientID)
	uploadBinary(binaryPath, cfg.Server+"/api/v1/clients/"+clientID+"/update", md5sum)
	fmt.Printf("更新已发送到客户端 %s\n", clientID)
}

func updateAllClients(binaryPath, md5sum string) {
	// 获取所有客户端
	resp, err := apiRequest("GET", "/api/v1/clients", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "获取客户端列表失败: %v\n", err)
		os.Exit(1)
	}

	var data struct {
		Clients []struct {
			ID string `json:"id"`
		} `json:"clients"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Clients) == 0 {
		fmt.Println("没有在线客户端")
		return
	}

	fmt.Printf("找到 %d 个在线客户端\n", len(data.Clients))
	for _, c := range data.Clients {
		fmt.Printf("  更新 %s...\n", c.ID)
		uploadBinary(binaryPath, cfg.Server+"/api/v1/clients/"+c.ID+"/update", md5sum)
	}
	fmt.Println("所有客户端更新已发送")
}

func cmdLogin(args ...string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	serverURL := fs.String("server", "", "服务器地址")
	apiKey := fs.String("api-key", "", "API 密钥")

	fs.Parse(args)

	if *serverURL == "" || *apiKey == "" {
		fmt.Fprintf(os.Stderr, "错误: -server 和 -api-key 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	url := *serverURL + "/api/v1/login"
	body := map[string]string{
		"api_key": *apiKey,
	}
	bodyData, _ := json.Marshal(body)

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(bodyData))
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// 检查 HTTP 状态码
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "登录失败 (HTTP %d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
		} `json:"data"`
	}
	json.Unmarshal(respBody, &result)

	if result.Code != 0 {
		fmt.Fprintf(os.Stderr, "登录失败: %s\n", result.Msg)
		os.Exit(1)
	}

	cfg.Server = *serverURL
	cfg.Token = result.Data.Token
	cfg.TokenExp = time.Now().Unix() + result.Data.ExpiresIn
	cfg.Insecure = cfg.Insecure || insecure

	configData, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, configData, 0600)

	fmt.Println("登录成功")
}

func cmdRegisterApply(args ...string) {
	fs := flag.NewFlagSet("register-apply", flag.ExitOnError)
	clientID := fs.String("id", "", "客户端 ID")
	pubKey := fs.String("pubkey", "", "公钥文件路径")
	token := fs.String("token", "", "认证 token")

	fs.Parse(args)

	if *clientID == "" || *pubKey == "" || *token == "" {
		fmt.Fprintf(os.Stderr, "错误: -id、-pubkey、-token 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	pubKeyData, err := os.ReadFile(*pubKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取公钥失败: %v\n", err)
		os.Exit(1)
	}

	body := map[string]string{
		"client_id":  *clientID,
		"public_key": string(pubKeyData),
		"token":      *token,
	}

	resp, err := apiRequest("POST", "/api/v1/register_apply", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "申请失败 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	fmt.Printf("申请已提交，等待管理员审核\n")
	fmt.Printf("客户端 ID: %s\n", *clientID)
}

func cmdRegisterAdd(args ...string) {
	fs := flag.NewFlagSet("register-add", flag.ExitOnError)
	clientID := fs.String("id", "", "客户端 ID")

	fs.Parse(args)

	if *clientID == "" {
		fmt.Fprintf(os.Stderr, "错误: -id 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	body := map[string]string{
		"client_id": *clientID,
	}

	resp, err := apiRequest("POST", "/api/v1/register_add", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "审核失败 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Certificate string `json:"certificate"`
		CertPrefix  string `json:"cert_prefix"`
		ClientID    string `json:"client_id"`
	}
	json.Unmarshal(resp.Data, &data)

	outFile := *clientID + ".crt"
	err = os.WriteFile(outFile, []byte(data.Certificate), 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "保存证书失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("审核通过，证书已签发\n")
	fmt.Printf("客户端 ID: %s\n", data.ClientID)
	fmt.Printf("证书前缀: %s\n", data.CertPrefix)
	fmt.Printf("证书已保存至: %s\n", outFile)
	fmt.Println("请妥善保管证书前缀，吊销时需要二次确认")
}

func cmdRegisterDelete(args ...string) {
	fs := flag.NewFlagSet("register-delete", flag.ExitOnError)
	clientID := fs.String("id", "", "客户端 ID")
	prefix := fs.String("prefix", "", "证书前40字符")

	fs.Parse(args)

	if *clientID == "" || *prefix == "" {
		fmt.Fprintf(os.Stderr, "错误: -id、-prefix 为必填参数\n")
		fs.Usage()
		os.Exit(1)
	}

	body := map[string]string{
		"client_id":   *clientID,
		"cert_prefix": *prefix,
	}

	resp, err := apiRequest("POST", "/api/v1/register_delete", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "吊销失败 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	fmt.Printf("客户端 %s 已移除\n", *clientID)
}

func cmdRegisterApplyList() {
	resp, err := apiRequest("GET", "/api/v1/register_apply_list", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Applications []struct {
			ClientID  string `json:"client_id"`
			CreatedAt int64  `json:"created_at"`
		} `json:"applications"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Applications) == 0 {
		fmt.Println("没有待审核的申请")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "客户端 ID\t申请时间\n")
	fmt.Fprintf(w, "----------\t--------\n")
	for _, app := range data.Applications {
		createdTime := time.Unix(app.CreatedAt, 0).Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "%s\t%s\n", app.ClientID, createdTime)
	}
	w.Flush()
}

func cmdRegisterInfo() {
	resp, err := apiRequest("GET", "/api/v1/register_info", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "请求失败: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误 [%d]: %s\n", resp.Code, resp.Msg)
		os.Exit(1)
	}

	var data struct {
		Clients []struct {
			ClientID   string `json:"client_id"`
			CertPrefix string `json:"cert_prefix"`
			ApprovedAt int64  `json:"approved_at"`
		} `json:"clients"`
	}
	json.Unmarshal(resp.Data, &data)

	if len(data.Clients) == 0 {
		fmt.Println("没有已通过的客户端")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "客户端 ID\t证书前缀\t通过时间\n")
	fmt.Fprintf(w, "----------\t----------\t--------\n")
	for _, c := range data.Clients {
		approvedTime := time.Unix(c.ApprovedAt, 0).Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "%s\t%s\t%s\n", c.ClientID, c.CertPrefix, approvedTime)
	}
	w.Flush()
}
