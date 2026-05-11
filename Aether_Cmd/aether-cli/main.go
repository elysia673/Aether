// Package main 是 Aether CLI 工具
//
// 用于管理 Aether 服务端，支持所有 API 操作。
// 默认配置文件：~/.aether_config.json
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	Server   string `json:"server"`   // 服务端地址，如 https://your-server.com:9909
	APIKey   string `json:"api_key"`  // API 访问密钥
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
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: cfg.Insecure},
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
  ping                          健康检查
  clients                       列出所有客户端
  info <client-id>              获取客户端代理信息
  proxies                       列出所有代理
  create <client-id> [options]  创建代理映射
  close <port>                  关闭代理
  relay <落地端A> <服务端B> [options]  A监听端口 → B的服务端口
  relay-sessions                  列出中继会话
  relay-close <session-id>        关闭中继会话
  help                          显示帮助

创建代理选项:
  -remote <port>      服务端暴露端口 (必填)
  -local <port>       客户端本地端口 (必填)
  -protocol <proto>   协议类型: tcp, udp, websocket (默认 tcp)
  -bind <addr>        服务端绑定地址 (默认 0.0.0.0)
  -local-ip <ip>      客户端本地 IP (默认 127.0.0.1)

创建中继选项:
  -source-port <port>     A端监听端口 (必填)，A 的用户访问此端口
  -target-port <port>     B端服务端口 (必填)，B 的真实服务端口
  -protocol <proto>       协议类型: tcp, udp, websocket (默认 tcp)
  -target-ip <ip>         B端服务 IP (默认 127.0.0.1)
  -source-ip <ip>         A端监听 IP (默认 0.0.0.0)

示例: A想访问B的80端口，A监听8090:
  aether-cli relay client-A client-B -source-port 8090 -target-port 80

全局选项:
  -config <path>      配置文件路径 (默认 ~/.aether_config.json)
  -json               JSON 输出模式
  -version			  版本
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
		return nil, fmt.Errorf("读取配置文件: %w", err)
	}

	var cfg CLIConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件: %w", err)
	}

	if cfg.Server == "" {
		return nil, fmt.Errorf("server 地址不能为空")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("api_key 不能为空")
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

	req.Header.Set("X-API-KEY", cfg.APIKey)
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
