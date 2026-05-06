package handler

import (
	"Aether/Server/manager"
	"Aether/pkg/model"
	"Aether/tools/mux"
	"Aether/tools/wsconn"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/gin-gonic/gin"
)

func (h *WSHandler) HandleTunnelWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("tunnel websocket upgrade error: %v", err)
		return
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("tunnel auth read error: %v", err)
		conn.Close()
		return
	}

	var auth model.TunnelAuthMsg
	if err := json.Unmarshal(msg, &auth); err != nil {
		log.Printf("tunnel auth json error: %v", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid auth"})
		conn.Close()
		return
	}

	if auth.Type != "tunnel_auth" {
		log.Printf("tunnel unexpected auth type: %s", auth.Type)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "unexpected message type"})
		conn.Close()
		return
	}

	table, key, err := h.clientMgr.FindTableByWSToken(auth.Data.Token)
	if err != nil {
		log.Printf("tunnel invalid token: %v", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid token"})
		conn.Close()
		return
	}

	wsAdapter := wsconn.New(conn)

	mx := mux.New(wsAdapter)

	ready := model.TunnelReadyMsg{
		Type: "tunnel_ready",
		Data: model.TunnelReadyData{Status: "ok"},
	}
	if err := conn.WriteJSON(ready); err != nil {
		log.Printf("tunnel ready write error: %v", err)
		mx.Close()
		return
	}

	table.PutMultiplexer(key, mx)
	log.Printf("Tunnel multiplexer created for key %s via WebSocket", key)

	<-mx.Done()
	table.RemoveMux(key, mx)
	table.RemoveWSToken(auth.Data.Token)
	log.Printf("Tunnel multiplexer closed for key %s", key)
}

// StartWSProxy 启动 WebSocket 代理监听
func (h *APIHandler) StartWSProxy(port int, bindAddr string, table *manager.ClientTable, token string) {
	key := table.TunnelKey(port)
	clientID := table.ClientID()

	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		log.Printf("ws proxy listen error: %v", err)
		return
	}
	defer ln.Close()

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
		table.RemoveWSToken(token)
	}()

	log.Printf("WebSocket proxy listening on :%d for %s", port, clientID)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		go handleWSConnection(conn, key, table)
	}
}

func handleWSConnection(conn net.Conn, key string, table *manager.ClientTable) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in handleWSConnection: %v", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("WebSocket proxy public connection from %s for %s", remoteAddr, key)

	mx, err := table.GetMultiplexer(key)
	if err != nil {
		log.Printf("No multiplexer available for %s: %v", key, err)
		conn.Close()
		return
	}

	channel, err := mx.CreateChannel()
	if err != nil {
		log.Printf("Failed to create channel for %s: %v", key, err)
		conn.Close()
		return
	}

	channel.RemoteAddr = conn.RemoteAddr()
	log.Printf("Channel %d created for public connection from %s", channel.ID, remoteAddr)

	go handleChannel(conn, channel, key)
}
