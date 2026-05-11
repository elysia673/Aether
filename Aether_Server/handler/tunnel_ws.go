package handler

import (
	"Aether/Aether_Server/manager"
	"Aether/common/model"
	"Aether/common/mux"
	"Aether/common/wsconn"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/gin-gonic/gin"
)

func (h *WSHandler) HandleTunnelWS(c *gin.Context) {
	conn, err := newUpgrader(h.domain).Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("隧道 WebSocket 升级错误: %v", err)
		return
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("隧道认证读取错误: %v", err)
		conn.Close()
		return
	}

	var auth model.TunnelAuthMsg
	if err := json.Unmarshal(msg, &auth); err != nil {
		log.Printf("隧道认证 JSON 错误: %v", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid auth"})
		conn.Close()
		return
	}

	if auth.Type != "tunnel_auth" {
		log.Printf("隧道认证类型异常: %s", auth.Type)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "unexpected message type"})
		conn.Close()
		return
	}

	table, key, err := h.clientMgr.FindTableByWSToken(auth.Data.Token)
	if err != nil {
		log.Printf("隧道令牌无效: %v", err)
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
		log.Printf("隧道就绪写入错误: %v", err)
		mx.Close()
		return
	}

	table.PutMultiplexer(key, mx)
	log.Printf("隧道多路复用器已创建，key=%s，通过 WebSocket", key)

	<-mx.Done()
	table.RemoveMux(key, mx)
	table.RemoveWSToken(auth.Data.Token)
	log.Printf("隧道多路复用器已关闭，key=%s", key)
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
		log.Printf("WebSocket 代理监听错误: %v", err)
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

	log.Printf("WebSocket 代理已监听端口 :%d，客户端 %s", port, clientID)

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
			log.Printf("handleWSConnection panic: %v", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("WebSocket 代理公网连接来自 %s，key=%s", remoteAddr, key)

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

	go handleChannel(conn, channel, key)
}
