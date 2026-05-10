package handler

import (
	"Aether/Aether_Server/manager"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

func (h *APIHandler) startUDPProxyListener(port int, clientID string) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("resolve error: %v", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("listen error: %v", err)
		return
	}
	defer conn.Close()

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		return
	}

	proxy := table.GetProxy(port)
	if proxy != nil {
		table.AddProxy(&manager.ProxyInfo{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			Listener:   conn,
		})
	}

	defer func() {
	}()

	log.Printf("UDP proxy listening on :%d", port)

	for {
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}
		if n == 0 {
			continue
		}

		go h.handleUDP(buf[0], conn, clientID, port)
	}
}

func (h *APIHandler) handleUDP(firstByte byte, publicConn *net.UDPConn, clientID string, remotePort int) {
	key := fmt.Sprintf("%s-%d", clientID, remotePort)

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		return
	}

	if firstByte == 'T' {
		publicConn.SetReadDeadline(time.Now().Add(10 * time.Second))
		rest := make([]byte, 6)
		if _, err := io.ReadFull(publicConn, rest); err != nil {
			log.Printf("UDP tunnel marker read error: %v", err)
			return
		}
		publicConn.SetReadDeadline(time.Time{})

		marker := string(append([]byte{firstByte}, rest...))
		if marker != "TUNNEL\n" {
			log.Printf("UDP invalid marker: %q", marker)
			return
		}

		table.SetUDPTunnel(key, publicConn)
		log.Printf("UDP tunnel registered for %s", key)

		buf := make([]byte, 1)
		publicConn.Read(buf)
		return
	}

	tunnel := table.GetUDPTunnel(key)
	if tunnel == nil {
		return
	}

	tunnel.Write([]byte{firstByte})
}

func (h *APIHandler) startUDPTunnelListener(port int, clientID string) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("UDP tunnel listen error: %v", err)
		return
	}
	defer ln.Close()

	log.Printf("UDP tunnel listener on :%d", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		go h.handleUDPTunnel(conn, clientID, port)
	}
}

func (h *APIHandler) handleUDPTunnel(conn net.Conn, clientID string, remotePort int) {
	defer conn.Close()

	buf := make([]byte, 7)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "TUNNEL\n" {
		return
	}

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		return
	}
	proxy := table.GetProxy(remotePort)
	if proxy == nil {
		return
	}
	localPort := proxy.LocalPort

	bindAddr := "127.0.0.1"

	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", localPort)))
	if err != nil {
		log.Printf("resolve local address error: %v", err)
		return
	}

	localConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("dial local error: %v", err)
		return
	}
	defer localConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(localConn, conn)
	}()

	go func() {
		defer wg.Done()
		io.Copy(conn, localConn)
	}()

	wg.Wait()
}
