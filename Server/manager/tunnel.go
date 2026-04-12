package manager

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"Aether/Server/model"
)

// Tunnel 表示一个数据隧道
type Tunnel struct {
	ID         string
	ClientID   string
	RemotePort int
	LocalPort  int
	Conn       net.Conn
	CreatedAt  int64
}

// TunnelManager 管理所有活跃隧道
type TunnelManager struct {
	tunnels sync.Map // key: tunnelID, value: *Tunnel
}

func NewTunnelManager() *TunnelManager {
	return &TunnelManager{}
}

func (tm *TunnelManager) Add(tunnelID string, tunnel *Tunnel) {
	tm.tunnels.Store(tunnelID, tunnel)
}

func (tm *TunnelManager) Remove(tunnelID string) {
	tm.tunnels.Delete(tunnelID)
}

func (tm *TunnelManager) Get(tunnelID string) (*Tunnel, bool) {
	val, ok := tm.tunnels.Load(tunnelID)
	if !ok {
		return nil, false
	}
	return val.(*Tunnel), true
}

// ForwardToClient 将公网连接的数据转发到 WebSocket 客户端
func (tm *TunnelManager) ForwardToClient(tunnelID string, clientMgr *ClientManager) {
	tunnel, ok := tm.Get(tunnelID)
	if !ok {
		log.Printf("tunnel %s not found", tunnelID)
		return
	}
	defer func() {
		tunnel.Conn.Close()
		tm.Remove(tunnelID)
		log.Printf("tunnel %s closed", tunnelID)
	}()

	// 获取客户端 WebSocket 连接
	conn, ok := clientMgr.Get(tunnel.ClientID)
	if !ok {
		log.Printf("client %s not found for tunnel %s", tunnel.ClientID, tunnelID)
		return
	}

	buf := make([]byte, 32*1024)
	for {
		// 设置读超时，避免长时间阻塞
		tunnel.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := tunnel.Conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("tunnel %s read error: %v", tunnelID, err)
			}
			break
		}

		// 封装为 tunnel_data 消息
		msg := model.WSMessage{
			Type: "tunnel_data",
			Data: map[string]interface{}{
				"tunnel_id": tunnelID,
				"data":      buf[:n],
			},
		}
		if err := conn.WriteJSON(msg); err != nil {
			log.Printf("tunnel %s write to ws error: %v", tunnelID, err)
			break
		}
	}
}
