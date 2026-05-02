# Aether

一个面向多客户端的内网穿透系统，提供精确的连接控制与动态路由能力

## 特性

- **多协议支持**: TCP、UDP、WebSocket 隧道模式
- **多路复用**: 单连接多通道，减少连接开销
- **自动重连**: 断线自动重连，隧道自动恢复
- **心跳检测**: WebSocket ping/pong + TCP keepalive
- **安全认证**: 魔数协议头 + 令牌认证
- **配置灵活**: JSON 配置文件 + 环境变量回退

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                          公网服务器                              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Aether Server                         │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │  │
│  │  │  API    │  │   WS    │  │   TCP   │  │   WS    │   │  │
│  │  │ Handler │  │ Handler │  │  Proxy  │  │ Tunnel  │   │  │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘   │  │
│  │       │            │            │            │         │  │
│  │  ┌────┴────────────┴────────────┴────────────┴────┐    │  │
│  │  │              Client Manager                    │    │  │
│  │  │  ┌─────────────────────────────────────────┐  │    │  │
│  │  │  │  Client Table (连接表)                   │  │    │  │
│  │  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐  │  │    │  │
│  │  │  │  │ Proxy 1 │ │ Proxy 2 │ │ Proxy 3 │  │  │    │  │
│  │  │  │  └─────────┘ └─────────┘ └─────────┘  │  │    │  │
│  │  │  └─────────────────────────────────────────┘  │    │  │
│  │  └───────────────────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────┘  │
│                          │                                   │
│                     :9909 (HTTPS/WSS)                        │
└──────────────────────────┼───────────────────────────────────┘
                           │
                           │ WebSocket 控制连接
                           │
┌──────────────────────────┼───────────────────────────────────┐
│                          │                                   │
│  ┌───────────────────────┴────────────────────────────────┐  │
│  │                  Aether Client                         │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐               │  │
│  │  │   WS    │  │ Message │  │  Tunnel │               │  │
│  │  │ Client  │──│ Handler │──│ Manager │               │  │
│  │  └─────────┘  └─────────┘  └────┬────┘               │  │
│  │                                  │                     │  │
│  └──────────────────────────────────┼─────────────────────┘  │
│                                     │                        │
│  ┌──────────────────────────────────┼─────────────────────┐  │
│  │  本地服务 (localhost:8080)        │                     │  │
│  └──────────────────────────────────┼─────────────────────┘  │
│                                     │                        │
└─────────────────────────────────────┼────────────────────────┘
                                      │
                                      │ TCP/WS 隧道
                                      │
┌─────────────────────────────────────┼────────────────────────┐
│                                     │                        │
│  ┌──────────────────────────────────┴─────────────────────┐  │
│  │                    内网设备                            │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │              本地服务                            │  │  │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐          │  │  │
│  │  │  │ Web App │ │   SSH   │ │ Database│          │  │  │
│  │  │  │  :8080  │ │  :22    │ │  :3306  │          │  │  │
│  │  │  └─────────┘ └─────────┘ └─────────┘          │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 连接流程

### 1. 客户端注册

```
Client                              Server
  │                                    │
  │──── WebSocket 连接 ───────────────>│
  │                                    │
  │──── register {client_id, token} ──>│
  │                                    │
  │<─── registered {server_host} ──────│
  │                                    │
  │<════ 心跳 ping/pong (30s) ═══════>│
```

### 2. 创建代理

```
User        API Server       Client
  │              │               │
  │── POST ─────>│               │
  │  /proxy      │               │
  │              │── proxy cmd ──>│
  │              │               │
  │              │               │── 启动隧道连接 ──>│
  │              │               │
  │<── {public_addr} ───────────│
```

### 3. TCP 隧道认证

```
Client                              Server
  │                                    │
  │──── TCP 连接 ─────────────────────>│
  │                                    │
  │──── [TUNL][len][token] ──────────>│
  │                                    │── 验证魔数+token
  │<──── [0x01] (ACK) ────────────────│
  │                                    │
  │<════ 多路复用数据传输 ═══════════>│
```

### 4. 公共连接转发

```
User                Server              Client          Local Service
  │                    │                   │                  │
  │── TCP 连接 ───────>│                   │                  │
  │                    │── 创建通道 ──────>│                  │
  │                    │                   │── 连接本地 ─────>│
  │                    │                   │                  │
  │<═══════════════════╪═══════════════════╪════════════════>│
  │                    │    多路复用数据    │                  │
```

### 5. UDP 代理

```
User                    Server                  Client              Local Service
  │                        │                       │                      │
  │── UDP 包 ─────────────>│                       │                      │
  │  (首字节='T')          │                       │                      │
  │                        │                       │                      │
  │── "TUNNEL\n" ─────────>│                       │                      │
  │                        │── 注册 UDP 隧道 ─────>│                      │
  │                        │                       │                      │
  │── UDP 数据包 ─────────>│                       │                      │
  │                        │── 转发到隧道 ────────>│── 转发到本地 ───────>│
  │                        │                       │                      │
  │<═══════════════════════╪═══════════════════════╪═════════════════════>│
  │                        │       UDP 数据        │                      │
```

UDP 隧道建立：
1. 首个 UDP 包首字节为 `'T'`
2. 完整标记为 `"TUNNEL\n"` (7字节)
3. 服务端注册 UDP 隧道
4. 后续数据包直接转发

## 快速开始

### 1. 配置服务端

创建 `Server/config.json`（可参考 `Server/config.example.json`）:

```json
{
  "server": {
    "addr": ":9909",
    "domain": "your-domain.com",
    "tunnel_port": 9908
  },
  "tls": {
    "enabled": true,
    "cert_file": "ssl/cert.pem",
    "key_file": "ssl/key.pem"
  },
  "auth": {
    "api_key": "your-api-key",
    "client_token": "your-client-token"
  },
  "storage": "data/proxies.json",
  "public_ip": ""
}
```

### 2. 启动服务端

```bash
cd Server
go run main.go -config config.json
```

### 3. 启动客户端

```bash
cd Client
export AETHER_WS_URL="wss://your-domain.com:9909/ws"
export AETHER_CLIENT_TOKEN="your-client-token"
go run main.go -id my-device
```

### 4. 创建代理

```bash
# TCP: 将本地 8080 端口映射到服务器 8080 端口
curl -X POST https://your-domain.com:9909/api/v1/clients/my-device/proxy \
  -H "X-API-KEY: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "remote_port": 8080,
    "local_port": 8080,
    "protocol": "tcp",
    "bind_addr": "0.0.0.0"
  }'

# UDP: 将本地 53 端口映射到服务器 5353 端口
curl -X POST https://your-domain.com:9909/api/v1/clients/my-device/proxy \
  -H "X-API-KEY: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "remote_port": 5353,
    "local_port": 53,
    "protocol": "udp",
    "bind_addr": "0.0.0.0"
  }'

# WebSocket: 将本地 3000 端口映射到服务器 3000 端口
curl -X POST https://your-domain.com:9909/api/v1/clients/my-device/proxy \
  -H "X-API-KEY: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "remote_port": 3000,
    "local_port": 3000,
    "protocol": "websocket",
    "bind_addr": "0.0.0.0"
  }'
```

## 命令行参数

### 服务端

```bash
./aether-server -config config.json
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-config` | `config.json` | 配置文件路径 |

### 客户端

```bash
./aether-client -config client.json
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-config` | `client.json` | 配置文件路径 |

### CLI 工具

```bash
# 配置文件 (~/.aether_config.json)
{
  "server": "https://your-domain.com:9909",
  "api_key": "your-api-key",
  "insecure": false
}

# 使用示例
aether-cli ping                           # 健康检查
aether-cli clients                        # 列出客户端
aether-cli info my-device                 # 查看代理信息
aether-cli proxies                        # 列出所有代理
aether-cli create my-device -remote 8080 -local 8080 -protocol tcp
aether-cli close 8080                     # 关闭代理

# JSON 输出模式
aether-cli -json clients
aether-cli -json info my-device
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-config` | `~/.aether_config.json` | 配置文件路径 |
| `-json` | `false` | JSON 输出模式 |

## 环境变量

| 变量 | 说明 |
|------|------|
| `AETHER_API_KEY` | API 访问密钥（配置文件优先） |
| `AETHER_CLIENT_TOKEN` | 客户端注册令牌（配置文件优先） |
| `AETHER_PUBLIC_IP` | 服务器公网 IP（配置文件优先） |
| `AETHER_WS_URL` | 客户端 WebSocket 地址 |
| `AETHER_TLS_SNI` | 客户端 TLS SNI 主机名（从 AETHER_WS_URL 解析） |
| `AETHER_ORIGIN` | 客户端 WebSocket Origin 请求头（优先于自动生成） |

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/PING` | 健康检查 |
| `GET` | `/api/v1/clients` | 列出所有客户端 |
| `GET` | `/api/v1/clients/:id/info` | 获取客户端代理信息 |
| `POST` | `/api/v1/clients/:id/proxy` | 创建代理映射 |
| `GET` | `/api/v1/proxies` | 列出所有代理 |
| `DELETE` | `/api/v1/proxies/:port` | 关闭代理 |

## 协议说明

### 隧道认证协议 (TCP)

```
┌──────────┬──────────┬──────────────────┐
│  Magic   │  Length  │      Token       │
│  4 bytes │  2 bytes │   N bytes        │
│  "TUNL"  │  N       │  认证令牌        │
└──────────┴──────────┴──────────────────┘
```

### UDP 隧道协议

```
┌──────────┬──────────────────┐
│  Marker  │     Data         │
│  7 bytes │   N bytes        │
│"TUNNEL\n"│  后续数据包      │
└──────────┴──────────────────┘
```

### 多路复用帧协议

```
┌──────────┬──────────┬──────────────────┐
│  ChanID  │  Length  │      Data        │
│  2 bytes │  2 bytes │   N bytes        │
│  通道ID  │  N       │  传输数据        │
└──────────┴──────────┴──────────────────┘
```

控制帧 (ChanID=0):

```
┌──────────┬──────────┬──────────────────┐
│ Command  │  ChanID  │                  │
│  1 byte  │  2 bytes │                  │
│  0x01=开  │  通道ID  │                  │
│  0x02=关  │          │                  │
└──────────┴──────────┴──────────────────┘
```

## 项目结构

```
Aether/
├── Server/                 # 服务端
│   ├── main.go            # 入口
│   ├── config.example.json # 配置示例
│   ├── config/            # 配置管理
│   ├── handler/           # 请求处理器
│   │   ├── api.go        # REST API
│   │   ├── ws.go         # WebSocket 注册
│   │   ├── tunnel_ws.go  # WebSocket 隧道
│   │   ├── tcp_proxy.go  # TCP 代理
│   │   └── udp_proxy.go  # UDP 代理
│   ├── manager/           # 连接管理
│   │   ├── client_manager.go
│   │   ├── client_table.go
│   │   └── connection.go
│   ├── middleware/         # 中间件
│   └── model/             # 数据模型
├── Client/                 # 客户端
│   ├── main.go            # 入口
│   ├── client.go          # 客户端核心
│   ├── handler.go         # 消息处理
│   ├── ports.go           # 端口扫描
│   └── utils.go           # 工具函数
├── cmd/                    # 命令行工具
│   └── aether-cli/        # CLI 管理工具
│       ├── main.go        # 入口
│       └── config.example.json
└── tools/                  # 共享工具
    ├── mux/               # 多路复用
    ├── proto/             # 协议定义
    └── wsconn/            # WebSocket 适配
```

## License

Apache License 2.0 — 详见 [LICENSE](LICENSE) 文件
