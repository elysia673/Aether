package handler

import (
	"Aether/Server/manager"
	"Aether/tools/mux"
	"Aether/tools/proto"
	"errors"
	"fmt"
	"log"
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
	log.Printf("Tunnel listener started on %s", tl.listener.Addr())

	for {
		conn, err := tl.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("tunnel accept error: %v", err)
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
			log.Printf("panic in handleTunnelConn: %v", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New tunnel connection from %s", remoteAddr)

	// 读取 token
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	token, err := proto.ReadTunnelAuth(conn)
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		log.Printf("tunnel auth failed from %s: %v", remoteAddr, err)
		conn.Close()
		return
	}

	// 查找对应的代理
	proxyKey, table := tl.findProxyByToken(token)
	if table == nil {
		log.Printf("tunnel token not found from %s", remoteAddr)
		conn.Close()
		return
	}

	// 发送 ACK
	if _, err := conn.Write([]byte{0x01}); err != nil {
		log.Printf("Failed to send ack to tunnel: %v", err)
		conn.Close()
		return
	}

	// 创建多路复用器
	mx := mux.New(conn)
	table.PutMultiplexer(proxyKey, mx)
	log.Printf("Tunnel multiplexer created for %s from %s", proxyKey, remoteAddr)

	<-mx.Done()
	table.RemoveMux(proxyKey)
	log.Printf("Tunnel multiplexer closed for %s", proxyKey)
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
