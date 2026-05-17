package handler

import (
	"Aether/Aether_Server/manager"
	alog "Aether/common/log"
	"Aether/common/mux"
	"Aether/common/proto"
	"errors"
	"fmt"
	"net"
	"time"
)

// TunnelListener 隧道监听器
//
// 监听隧道端口，接受客户端隧道连接。
// 客户端连接后发送 token，服务端找到对应的代理并创建多路复用器。
type TunnelListener struct {
	listener  net.Listener
	clientMgr *manager.ClientManager
}

// NewTunnelListener 创建隧道监听器
func NewTunnelListener(port int, clientMgr *manager.ClientManager) (*TunnelListener, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen tunnel port: %w", err)
	}

	return &TunnelListener{
		listener:  ln,
		clientMgr: clientMgr,
	}, nil
}

// Start 启动隧道监听器
func (tl *TunnelListener) Start() {
	alog.Info(alog.CatTunnel, "tunnel listener started", "addr", tl.listener.Addr())

	for {
		conn, err := tl.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			alog.Error(alog.CatTunnel, "tunnel accept error", "error", err)
			continue
		}

		go tl.handleTunnelConn(conn)
	}
}

// Close 关闭隧道监听器
func (tl *TunnelListener) Close() {
	tl.listener.Close()
}

// handleTunnelConn 处理隧道连接
func (tl *TunnelListener) handleTunnelConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "handleTunnelConn panic", "error", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatTunnel, "new tunnel connection", "remote", remoteAddr)

	// 读取 token
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	token, err := proto.ReadTunnelAuth(conn)
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		alog.Error(alog.CatAuth, "tunnel auth failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	// 查找对应的代理
	proxyKey, table := tl.findProxyByToken(token)
	if table == nil {
		alog.Warn(alog.CatAuth, "tunnel token not found", "remote", remoteAddr)
		conn.Close()
		return
	}

	// 发送 ACK
	if _, err := conn.Write([]byte{0x01}); err != nil {
		alog.Error(alog.CatTunnel, "send tunnel ack failed", "error", err)
		conn.Close()
		return
	}

	// 创建多路复用器
	mx := mux.New(conn)
	table.PutMultiplexer(proxyKey, mx)
	alog.Info(alog.CatMux, "tunnel multiplexer created", "key", proxyKey, "remote", remoteAddr)

	<-mx.Done()
	table.RemoveMux(proxyKey, mx)
	alog.Info(alog.CatMux, "tunnel multiplexer closed", "key", proxyKey)
}

// findProxyByToken 根据 token 查找代理
func (tl *TunnelListener) findProxyByToken(token string) (string, *manager.ClientTable) {
	var foundKey string
	var foundTable *manager.ClientTable

	tl.clientMgr.RangeClients(func(clientID string, table *manager.ClientTable) bool {
		key, err := table.GetTunnelToken(token)
		if err == nil {
			foundKey = key
			foundTable = table
			return false
		}
		return true
	})

	return foundKey, foundTable
}
