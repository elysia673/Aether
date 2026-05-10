package handler

import (
	"Aether/Aether_Server/manager"
	"Aether/common/mux"
	"Aether/common/proto"
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// StartTCPProxy 启动 TCP 代理监听
func (h *APIHandler) StartTCPProxy(port int, bindAddr string, table *manager.ClientTable, token string) {
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		log.Printf("监听错误: %v", err)
		return
	}
	defer ln.Close()

	key := table.TunnelKey(port)
	clientID := table.ClientID()

	proxy := table.GetProxy(port)
	if proxy != nil {
		table.AddProxy(&manager.ProxyInfo{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			Listener:   ln,
		})
	}

	defer func() {
		table.RemoveTunnel(key)
	}()

	table.SetPending(key)
	log.Printf("TCP 代理已监听端口 :%d，客户端 %s", port, clientID)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}

		go handleTCPConnection(conn, key, token, table)
	}
}

func handleTCPConnection(conn net.Conn, key string, token string, table *manager.ClientTable) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("handleTCPConnection panic: %v", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("新的 TCP 连接来自 %s，key=%s", remoteAddr, key)

	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	if proto.IsTunnelConn(br) {
		receivedToken, err := proto.ReadTunnelAuth(br)
		conn.SetReadDeadline(time.Time{})

		if err == nil && receivedToken == token {
			log.Printf("隧道连接已建立，key=%s，来自 %s，创建多路复用器", key, remoteAddr)

			mx := mux.New(conn)

			if _, err := conn.Write([]byte{0x01}); err != nil {
				log.Printf("发送隧道确认失败: %v", err)
				conn.Close()
				return
			}

			table.PutMultiplexer(key, mx)
			log.Printf("多路复用器已创建，key=%s", key)

			<-mx.Done()
			table.RemoveMux(key, mx)
			log.Printf("多路复用器已关闭，key=%s", key)
			return
		}

		if err != nil {
			log.Printf("隧道认证失败，来自 %s: %v", remoteAddr, err)
		} else {
			log.Printf("令牌不匹配，来自 %s", remoteAddr)
		}
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Time{})
	log.Printf("公网连接来自 %s，key=%s，获取多路复用器", remoteAddr, key)

	mx, err := table.GetMultiplexer(key)
	if err != nil {
		log.Printf("没有可用的多路复用器，key=%s: %v", key, err)
		conn.Close()
		return
	}

	channel, err := mx.CreateChannel()
	if err != nil {
		log.Printf("创建通道失败，key=%s: %v", key, err)
		conn.Close()
		return
	}

	channel.RemoteAddr = conn.RemoteAddr()
	log.Printf("通道 %d 已创建，公网连接来自 %s", channel.ID, remoteAddr)

	wrappedConn := &bufferedConn{reader: br, conn: conn}
	go handleChannel(wrappedConn, channel, key)
}

type bufferedConn struct {
	reader io.Reader
	conn   net.Conn
}

func (bc *bufferedConn) Read(b []byte) (int, error) {
	return bc.reader.Read(b)
}

func (bc *bufferedConn) Write(b []byte) (int, error) {
	return bc.conn.Write(b)
}

func (bc *bufferedConn) Close() error {
	return bc.conn.Close()
}

func (bc *bufferedConn) LocalAddr() net.Addr {
	return bc.conn.LocalAddr()
}

func (bc *bufferedConn) RemoteAddr() net.Addr {
	return bc.conn.RemoteAddr()
}

func (bc *bufferedConn) SetDeadline(t time.Time) error {
	return bc.conn.SetDeadline(t)
}

func (bc *bufferedConn) SetReadDeadline(t time.Time) error {
	return bc.conn.SetReadDeadline(t)
}

func (bc *bufferedConn) SetWriteDeadline(t time.Time) error {
	return bc.conn.SetWriteDeadline(t)
}

func handleChannel(publicConn net.Conn, channel *mux.Channel, key string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in handleChannel: %v", r)
		}
		publicConn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in handleChannel publicRead goroutine: %v", r)
			}
			wg.Done()
		}()
		buf := make([]byte, 4096)
		for {
			n, err := publicConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("Channel %d: public read error: %v", channel.ID, err)
				}
				break
			}

			if err := channel.Mux.Send(channel.ID, buf[:n]); err != nil {
				log.Printf("Channel %d: send error: %v", channel.ID, err)
				break
			}
		}

		channel.Mux.CloseChannel(channel.ID)
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in handleChannel publicWrite goroutine: %v", r)
			}
			wg.Done()
		}()
		for {
			data, ok := channel.ReceiveBlocking()
			if !ok {
				break
			}

			if _, err := publicConn.Write(data); err != nil {
				log.Printf("Channel %d: public write error: %v", channel.ID, err)
				break
			}
		}
	}()

	wg.Wait()
	log.Printf("通道 %d 已关闭，key=%s", channel.ID, key)
}
