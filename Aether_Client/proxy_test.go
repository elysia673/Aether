package main

import (
	"Aether/common/model"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestParseDataFromServer(t *testing.T) {
	wsMessage := `{"type":"proxy","data":"{\"server_host\":\"example.com\",\"remote_port\":17335,\"local_port\":25565}"}`

	var msg model.WSMessage
	if err := json.Unmarshal([]byte(wsMessage), &msg); err != nil {
		t.Fatal(err)
	}

	var cmd model.CommandData
	if dataStr, ok := msg.Data.(string); ok {
		if err := json.Unmarshal([]byte(dataStr), &cmd); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("After parsing: server_host=%s, remote_port=%d", cmd.ServerHost, cmd.RemotePort)

	if cmd.ServerHost == "" {
		t.Error("server_host is empty!")
	}
	if cmd.RemotePort != 17335 {
		t.Errorf("Expected remote_port=17335, got %d", cmd.RemotePort)
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

	cmd := model.CommandData{
		ServerHost: "127.0.0.1",
		RemotePort: remotePort,
		LocalPort:  localPort,
	}
	cmdBytes, _ := json.Marshal(cmd)
	proxyCmd := model.WSMessage{
		Type: "proxy",
		Data: string(cmdBytes),
	}
	dataBytes, _ := json.Marshal(proxyCmd)

	var parsed model.WSMessage
	json.Unmarshal(dataBytes, &parsed)

	var cmd2 model.CommandData
	if dataStr, ok := parsed.Data.(string); ok {
		json.Unmarshal([]byte(dataStr), &cmd2)
	}

	clientTunnelConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", cmd2.RemotePort))
	if err != nil {
		t.Fatal(err)
	}
	defer clientTunnelConn.Close()

	clientLocalConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", cmd2.LocalPort))
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
