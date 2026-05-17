package handler

import (
	"Aether/Aether_Server/manager"
	alog "Aether/common/log"
	"Aether/common/mux"
	"Aether/common/proto"
	"bufio"
	"errors"
	"fmt"
	"io"
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
		alog.Error(alog.CatProxy, "listen failed", "error", err, "port", port)
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
	alog.Info(alog.CatProxy, "TCP proxy listening", "port", port, "client_id", clientID)

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
			alog.Error(alog.CatProxy, "handleTCPConnection panic", "error", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatProxy, "new TCP connection", "remote", remoteAddr, "key", key)

	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	if proto.IsTunnelConn(br) {
		receivedToken, err := proto.ReadTunnelAuth(br)
		conn.SetReadDeadline(time.Time{})

		if err == nil && receivedToken == token {
			alog.Info(alog.CatTunnel, "tunnel connection established, creating multiplexer", "key", key, "remote", remoteAddr)

			mx := mux.New(&bufferedConn{reader: br, conn: conn})

			if _, err := conn.Write([]byte{0x01}); err != nil {
				alog.Error(alog.CatTunnel, "send tunnel ack failed", "error", err)
				conn.Close()
				return
			}

			table.PutMultiplexer(key, mx)
			alog.Info(alog.CatMux, "multiplexer created", "key", key)

			<-mx.Done()
			table.RemoveMux(key, mx)
			alog.Info(alog.CatMux, "multiplexer closed", "key", key)
			return
		}

		if err != nil {
			alog.Error(alog.CatAuth, "tunnel auth failed", "remote", remoteAddr, "error", err)
		} else {
			alog.Warn(alog.CatAuth, "token mismatch", "remote", remoteAddr)
		}
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Time{})
	alog.Info(alog.CatProxy, "public connection, getting multiplexer", "remote", remoteAddr, "key", key)

	mx, err := table.GetMultiplexer(key)
	if err != nil {
		alog.Error(alog.CatMux, "no multiplexer available", "key", key, "error", err)
		conn.Close()
		return
	}

	port := table.GetProxyPort(key)
	if port == 0 {
		alog.Error(alog.CatProxy, "cannot get port", "key", key)
		conn.Close()
		return
	}

	channel, err := mx.OpenChannel(uint16(port))
	if err != nil {
		alog.Error(alog.CatMux, "create channel failed", "key", key, "error", err)
		conn.Close()
		return
	}

	alog.Info(alog.CatMux, "channel opened", "port", channel.Port, "remote", remoteAddr)

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
			alog.Error(alog.CatMux, "handleChannel panic", "error", r, "port", channel.Port)
		}
		publicConn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// 用户 → 隧道（优先 splice 零拷贝）
	go func() {
		defer func() {
			if r := recover(); r != nil {
				alog.Error(alog.CatMux, "handleChannel publicRead panic", "error", r, "port", channel.Port)
			}
			publicConn.Close()
			wg.Done()
		}()

		// 尝试 splice
		if tc, ok := publicConn.(*net.TCPConn); ok && channel.Mux.SpliceAvailable() {
			if f, err := tc.File(); err == nil {
				fd := int(f.Fd())
				defer f.Close()
				for {
					if err := channel.Mux.SpliceSend(channel.Port, fd, mux.MaxFrameSize); err != nil {
						break
					}
				}
				channel.Mux.CloseChannel(channel.Port)
				return
			}
		}

		// 回退：Read→Send
		buf := make([]byte, mux.MaxFrameSize)
		for {
			n, err := publicConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					alog.Error(alog.CatMux, "public read error", "port", channel.Port, "error", err)
				}
				break
			}
			if err := channel.Mux.Send(channel.Port, buf[:n]); err != nil {
				alog.Error(alog.CatMux, "mux send error", "port", channel.Port, "error", err)
				break
			}
		}
		channel.Mux.CloseChannel(channel.Port)
	}()

	// 隧道 → 用户
	go func() {
		defer func() {
			if r := recover(); r != nil {
				alog.Error(alog.CatMux, "handleChannel publicWrite panic", "error", r, "port", channel.Port)
			}
			publicConn.Close()
			wg.Done()
		}()
		for {
			data, ok := channel.ReceiveBlocking()
			if !ok {
				break
			}
			if _, err := publicConn.Write(data); err != nil {
				alog.Error(alog.CatMux, "public write error", "port", channel.Port, "error", err)
				break
			}
			// 消费数据后发送窗口更新
			channel.Mux.SendWindowUpdate(channel.Port, len(data))
		}
	}()

	wg.Wait()
	alog.Info(alog.CatMux, "channel closed", "port", channel.Port, "key", key)
}
