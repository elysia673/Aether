# Aether

一个面向多客户端的内网穿透系统，提供精确的连接控制与动态路由能力

## 特性

- **多协议支持**: TCP、UDP、WebSocket 隧道模式
- **多路复用**: 单连接多通道，减少连接开销
- **自动重连**: 断线自动重连，隧道自动恢复
- **心跳检测**: WebSocket ping/pong + TCP keepalive
- **安全认证**: mTLS 客户端证书 + JWT Token + API Key 三重认证
- **配置灵活**: JSON 配置文件 + 环境变量回退
- **客户端中继**: 支持两个客户端之间通过服务器中继连接
- **Docker 一键部署**: 自签名/本地证书两种部署脚本

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
                           │ WebSocket 控制连接 (mTLS)
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

## 快速开始

### Docker 一键部署（推荐）

#### 自签名证书

```bash
# 构建镜像 + 生成证书和配置
./docker/build.sh xxx.xxx.xxx.xxx

# 部署服务端
./docker/deploy.sh xxx.xxx.xxx.xxx
```

#### 本地证书

```bash
# 构建镜像 + 使用已有证书
./docker/build-cert.sh xxx.xxx.xxx.xxx /path/to/cert.pem /path/to/key.pem

# 部署服务端
./docker/deploy-cert.sh xxx.xxx.xxx.xxx /path/to/cert.pem /path/to/key.pem
```

#### 部署客户端

部署完成后，脚本会输出客户端部署命令，直接复制执行：

```bash
./docker/deploy-client.sh wss://xxx.xxx.xxx.xxx:9909 "<client-token>" my-device
```

#### 本地证书

```bash
# 构建镜像 + 使用已有证书
./build-cert.sh elysia.media /path/to/cert.pem /path/to/key.pem

# 部署服务端
./deploy-cert.sh elysia.media /path/to/cert.pem /path/to/key.pem
```

#### 部署客户端

部署完成后，脚本会输出客户端部署命令，直接复制执行：

```bash
./deploy-client.sh wss://xxx.xxx.xxx.xxx:9909 "<client-token>" my-device
```

首次部署需要注册客户端：

```bash
# 查看公钥
cat deploy-client/data/client.pub

# 提交注册申请
aether-cli register apply -id my-device -pubkey deploy-client/data/client.pub -token "<client-token>"

# 审核通过并签发证书
aether-cli register add -id my-device
```

### 手动部署

#### 1. 配置服务端

创建 `config.json`：

```json
{
  "server": {
    "addr": ":9909",
    "domain": "xxx.xxx.xxx.xxx",
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
  "data_dir": "data",
  "log_path": "data/server.log",
  "public_ip": ""
}
```

#### 2. 启动服务端

```bash
./aether-server -config config.json
```

#### 3. CLI 登录

```bash
aether-cli login -server https://xxx.xxx.xxx.xxx:9909 -api-key "your-api-key"
```

#### 4. 创建代理

```bash
# TCP: 将本地 8080 端口映射到服务器 8080 端口
aether-cli create my-device -remote 8080 -local 8080 -protocol tcp

# UDP: 将本地 53 端口映射到服务器 5353 端口
aether-cli create my-device -remote 5353 -local 53 -protocol udp

# WebSocket: 将本地 3000 端口映射到服务器 3000 端口
aether-cli create my-device -remote 3000 -local 3000 -protocol websocket
```

## 认证流程

```
                        ┌──────────────┐
                        │  API Key     │
                        │ (config.json)│
                        └──────┬───────┘
                               │
                        POST /api/v1/login
                               │
                        ┌──────▼───────┐
                        │   JWT Token  │
                        │  (1年有效)   │
                        └──────┬───────┘
                               │
                        Authorization: Bearer <token>
                               │
                        ┌──────▼───────┐
                        │  API 接口    │
                        └──────────────┘

客户端连接:
                        ┌──────────────┐
                        │ 客户端证书   │
                        │ (mTLS, 1年)  │
                        └──────┬───────┘
                               │
                        TLS 握手 + 证书校验
                               │
                        ┌──────▼───────┐
                        │ 注册表校验   │
                        │ (CN + 状态)  │
                        └──────┬───────┘
                               │
                        WebSocket 连接
```

## CLI 工具

```bash
# 登录
aether-cli login -server https://xxx.xxx.xxx.xxx:9909 -api-key "your-api-key"

# 客户端管理
aether-cli clients                        # 列出客户端
aether-cli info my-device                 # 查看代理信息

# 代理管理
aether-cli proxies                        # 列出所有代理
aether-cli create my-device -remote 8080 -local 8080 -protocol tcp
aether-cli close 8080                     # 关闭代理

# 注册管理
aether-cli register apply -id my-device -pubkey my.pub -token <token>
aether-cli register add -id my-device
aether-cli register delete -id my-device -prefix <cert-prefix>
aether-cli register apply_list            # 待审核列表
aether-cli register info                  # 已通过列表

# 中继管理
aether-cli relay client-A client-B -source-port 8090 -target-port 80
aether-cli relay-sessions                 # 列出中继会话
aether-cli relay-close <session-id>       # 关闭中继会话

# 其他
aether-cli ping                           # 健康检查
aether-cli -json clients                  # JSON 输出模式
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-config` | `~/.aether_config.json` | 配置文件路径 |
| `-json` | `false` | JSON 输出模式 |
| `-version` | `false` | 版本信息 |

## API 接口

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| `GET` | `/PING` | 无 | 健康检查 |
| `POST` | `/api/v1/login` | API Key | 登录获取 JWT Token |
| `POST` | `/api/v1/register_apply` | 无 | 提交注册申请 |
| `GET` | `/api/v1/register_info` | 无 | 查看已通过列表 |
| `POST` | `/api/v1/register_add` | JWT | 审核通过并签发证书 |
| `POST` | `/api/v1/register_delete` | JWT | 吊销客户端证书 |
| `GET` | `/api/v1/register_apply_list` | JWT | 查看待审核列表 |
| `GET` | `/api/v1/clients` | JWT | 列出所有客户端 |
| `GET` | `/api/v1/clients/:id/info` | JWT | 获取客户端代理信息 |
| `POST` | `/api/v1/clients/:id/proxy` | JWT | 创建代理映射 |
| `GET` | `/api/v1/proxies` | JWT | 列出所有代理 |
| `DELETE` | `/api/v1/proxies/:port` | JWT | 关闭代理 |
| `POST` | `/api/v1/relay/connect` | JWT | 创建中继连接 |
| `GET` | `/api/v1/relay/sessions` | JWT | 列出中继会话 |
| `DELETE` | `/api/v1/relay/sessions/:id` | JWT | 关闭中继会话 |

## 环境变量

### 服务端

| 变量 | 说明 |
|------|------|
| `AETHER_API_KEY` | API 访问密钥 |
| `AETHER_CLIENT_TOKEN` | 客户端注册令牌 |
| `AETHER_SERVER_ADDR` | 监听地址 |
| `AETHER_DOMAIN` | 公网域名 |
| `AETHER_TUNNEL_PORT` | 隧道端口 |
| `AETHER_TLS_CERT` | TLS 证书路径 |
| `AETHER_TLS_KEY` | TLS 私钥路径 |
| `AETHER_STORAGE` | 存储文件路径 |
| `AETHER_DATA_DIR` | 数据目录 |
| `AETHER_LOG_PATH` | 日志文件路径 |
| `AETHER_PUBLIC_IP` | 公网 IP |

### 客户端

| 变量 | 说明 |
|------|------|
| `AETHER_WS_URL` | WebSocket 地址 |
| `AETHER_CLIENT_TOKEN` | 客户端注册令牌 |
| `AETHER_CLIENT_ID` | 客户端 ID |
| `AETHER_USE_HTTP` | 使用 HTTP 模式 |
| `AETHER_TLS_SNI` | TLS SNI 主机名 |
| `AETHER_ORIGIN` | WebSocket Origin 请求头 |
| `AETHER_RECONNECT_DELAY` | 重连延迟（秒） |
| `AETHER_LOG_PATH` | 日志文件路径 |

## 安全机制

| 层级 | 机制 | 说明 |
|------|------|------|
| 传输层 | TLS 1.2+ | 所有通信加密 |
| 客户端认证 | mTLS 客户端证书 | CA 签发，1年有效 |
| API 认证 | JWT Token | 启动时随机生成密钥，1年有效 |
| 登录认证 | API Key | 常量时间比较，防时序攻击 |
| 暴力防护 | Rate Limit | 登录 10次/分钟，API 60次/分钟 |
| WebSocket | Origin 校验 | 仅允许配置的域名/IP |
| 证书吊销 | 注册表即时生效 | 删除后立即拒绝连接 |

## 连接流程

### 1. 客户端注册

```
Client                              Server
  │                                    │
  │──── TLS 握手 (mTLS) ─────────────>│
  │                                    │── 验证客户端证书
  │                                    │── 校验注册表 CN + 状态
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

### 3. 客户端中继

```
User           Server              Client A            Client B
  │               │                    │                   │
  │── POST ──────>│                    │                   │
  │  /relay/      │                    │                   │
  │  connect      │                    │                   │
  │               │── relay signal ───>│                   │
  │               │── relay signal ───────────────────────>│
  │               │                    │                   │
  │               │<═══════════════════╪══════════════════>│
  │               │      WebSocket 中继连接                 │
```

## 协议说明

### 隧道认证协议 (TCP)

```
┌──────────┬──────────┬──────────────────┐
│  Magic   │  Length  │      Token       │
│  4 bytes │  2 bytes │   N bytes        │
│  "TUNL"  │  N       │  认证令牌         │
└──────────┴──────────┴──────────────────┘
```

### 多路复用帧协议

```
┌──────────┬──────────┬──────────────────┐
│  ChanID  │  Length  │      Data        │
│  2 bytes │  2 bytes │   N bytes        │
│  通道ID  │  N       │  传输数据        │
└──────────┴──────────┴──────────────────┘
```

## 项目结构

```
Aether/
├── Aether_Server/              # 服务端
│   ├── main.go                 # 入口
│   ├── handler/                # 请求处理器
│   │   ├── api.go              # REST API
│   │   ├── ws.go               # WebSocket 注册
│   │   ├── tunnel_ws.go        # WebSocket 隧道
│   │   ├── tcp_proxy.go        # TCP 代理
│   │   ├── udp_proxy.go        # UDP 代理
│   │   └── relay.go            # 客户端中继
│   ├── manager/                # 连接管理
│   ├── middleware/             # 中间件 (JWT, Auth, RateLimit)
│   ├── register/               # CA 证书管理
│   └── storage/                # 持久化存储
├── Aether_Client/              # 客户端
│   ├── main.go                 # 入口
│   ├── client.go               # 客户端核心
│   ├── conn/                   # WebSocket 连接
│   └── handler/                # 消息处理
├── Aether_Cmd/                 # 命令行工具
│   └── aether-cli/             # CLI 管理工具
├── common/                     # 共享代码
│   ├── config/                 # 配置管理
│   ├── model/                  # 数据模型
│   ├── mux/                    # 多路复用
│   └── wsconn/                 # WebSocket 适配
└── docker/                     # Docker 构建与部署
    ├── Dockerfile              # 服务端镜像
    ├── Dockerfile.client       # 客户端镜像
    ├── build.sh                # 构建脚本 (自签名证书)
    ├── build-cert.sh           # 构建脚本 (本地证书)
    ├── build2.sh               # 多平台构建脚本
    ├── deploy.sh               # 部署脚本 (自签名证书)
    ├── deploy-cert.sh          # 部署脚本 (本地证书)
    └── deploy-client.sh        # 客户端部署脚本
```

## License

Apache License 2.0 — 详见 [LICENSE](LICENSE) 文件
