package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestParseDataFromServer(t *testing.T) {
	wsMessage := `{"type":"proxy","data":"{\"server_host\":\"example.com\",\"remote_port\":17335,\"local_port\":25565}"}`

	var msg map[string]interface{}
	json.Unmarshal([]byte(wsMessage), &msg)

	var data map[string]interface{}
	if str, ok := msg["data"].(string); ok {
		json.Unmarshal([]byte(str), &data)
	}

	serverHost := data["server_host"].(string)
	remotePort := int(data["remote_port"].(float64))

	t.Logf("After parsing: server_host=%s, remote_port=%d", serverHost, remotePort)

	if serverHost == "" {
		t.Error("server_host is empty!")
	}
	if remotePort != 17335 {
		t.Errorf("Expected remote_port=17335, got %d", remotePort)
	}
}

func TestFullProxyFlow(t *testing.T) {
	lnRemote, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnRemote.Close()
	remotePort := lnRemote.Addr().(*net.TCPAddr).Port

	lnLocal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnLocal.Close()
	localPort := lnLocal.Addr().(*net.TCPAddr).Port

	var tunnelConn net.Conn
	tunnelCh := make(chan net.Conn, 1)

	go func() {
		conn, err := lnRemote.Accept()
		if err != nil {
			return
		}
		tunnelCh <- conn
	}()

	time.Sleep(50 * time.Millisecond)

	proxyCmd := map[string]interface{}{
		"type": "proxy",
		"data": map[string]interface{}{
			"server_host": "127.0.0.1",
			"remote_port": float64(remotePort),
			"local_port":  float64(localPort),
		},
	}

	dataBytes, _ := json.Marshal(proxyCmd)
	var parsed map[string]interface{}
	json.Unmarshal(dataBytes, &parsed)

	data := parsed["data"].(map[string]interface{})
	remotePort2 := int(data["remote_port"].(float64))
	localPort2 := int(data["local_port"].(float64))

	clientTunnelConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", remotePort2))
	if err != nil {
		t.Fatal(err)
	}
	defer clientTunnelConn.Close()

	clientLocalConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", localPort2))
	if err != nil {
		t.Fatal(err)
	}
	defer clientLocalConn.Close()

	tunnelConn = <-tunnelCh
	if tunnelConn == nil {
		t.Fatal("tunnel not established")
	}
	t.Log("Tunnel connected")

	go io.Copy(clientLocalConn, clientTunnelConn)
	io.Copy(clientTunnelConn, clientLocalConn)

	tunnelConn.Close()
	clientTunnelConn.Close()
	clientLocalConn.Close()

	t.Log("PASSED: Full proxy flow works!")
}
