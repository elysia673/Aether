package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// MessageHandler 处理服务端下发的消息
type MessageHandler struct {
	client *Client
}

func NewMessageHandler(c *Client) *MessageHandler {
	return &MessageHandler{client: c}
}

// Handle 消息分发
func (h *MessageHandler) Handle(msg map[string]interface{}) {
	msgType, ok := msg["type"].(string)
	if !ok {
		log.Println("message missing type field")
		return
	}

	switch msgType {
	case "list_ports": // 确保端口查询功能也正常
		h.handleListPorts(msg)
	case "new_tunnel": // ✅ 关键修复：处理新建隧道命令
		h.handleNewTunnel(msg)
	case "tunnel_data":
		h.handleTunnelData(msg)
	case "proxy":
		log.Printf("received proxy command: %+v", msg)
	case "ping":
		// 心跳，可忽略
	default:
		log.Printf("unknown message type: %s", msgType)
	}
}

func (h *MessageHandler) handleListPorts(msg map[string]interface{}) {
	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		log.Println("invalid list_ports data format")
		return
	}
	requestID, _ := data["request_id"].(string)

	ports, err := GetListeningPorts()
	respData := map[string]interface{}{
		"request_id": requestID,
		"ports":      ports,
	}
	if err != nil {
		respData["error"] = err.Error()
	}

	resp := map[string]interface{}{
		"type": "ports_list",
		"data": respData,
	}
	if err := h.client.WriteJSON(resp); err != nil {
		log.Printf("failed to send ports_list: %v", err)
	}
}

func (h *MessageHandler) handleNewTunnel(msg map[string]interface{}) {
	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		log.Println("invalid new_tunnel data")
		return
	}
	tunnelID, _ := data["tunnel_id"].(string)
	localPort, _ := data["local_port"].(float64) // JSON 数字默认 float64
	if tunnelID == "" || localPort == 0 {
		log.Printf("invalid tunnel params: id=%s, port=%v", tunnelID, localPort)
		return
	}

	// 连接本地服务
	localAddr := fmt.Sprintf("127.0.0.1:%d", int(localPort))
	localConn, err := net.Dial("tcp", localAddr)
	if err != nil {
		log.Printf("failed to connect to local %s: %v", localAddr, err)
		// 可选：向服务端发送错误消息
		return
	}

	// 存储隧道连接
	h.client.tunnels.Store(tunnelID, localConn)
	log.Printf("tunnel %s established to %s", tunnelID, localAddr)

	// 启动 goroutine 从本地连接读取数据并发送到服务端
	go h.forwardLocalToServer(tunnelID, localConn)
}

func (h *MessageHandler) forwardLocalToServer(tunnelID string, localConn net.Conn) {
	defer func() {
		localConn.Close()
		h.client.tunnels.Delete(tunnelID)
		log.Printf("tunnel %s closed", tunnelID)
	}()

	buf := make([]byte, 32*1024)
	for {
		localConn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := localConn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("tunnel %s local read error: %v", tunnelID, err)
			}
			break
		}

		msg := map[string]interface{}{
			"type": "tunnel_data",
			"data": map[string]interface{}{
				"tunnel_id": tunnelID,
				"data":      base64.StdEncoding.EncodeToString(buf[:n]),
			},
		}
		if err := h.client.WriteJSON(msg); err != nil {
			log.Printf("tunnel %s write to ws error: %v", tunnelID, err)
			break
		}
	}
}

func (h *MessageHandler) handleTunnelData(msg map[string]interface{}) {
	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		log.Println("invalid tunnel_data format")
		return
	}
	tunnelID, _ := data["tunnel_id"].(string)
	encodedData, _ := data["data"].(string) // 服务端将 []byte 编码为了 base64 字符串

	val, ok := h.client.tunnels.Load(tunnelID)
	if !ok {
		log.Printf("tunnel %s not found", tunnelID)
		return
	}
	localConn := val.(net.Conn)

	// base64 解码
	decoded, err := base64.StdEncoding.DecodeString(encodedData)
	if err != nil {
		log.Printf("tunnel %s base64 decode error: %v", tunnelID, err)
		return
	}

	_, err = localConn.Write(decoded)
	if err != nil {
		log.Printf("tunnel %s write to local error: %v", tunnelID, err)
		localConn.Close()
		h.client.tunnels.Delete(tunnelID)
	}
}
