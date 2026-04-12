package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	serverURL      = "ws://www.elysia.media:9909/ws"
	clientID       = "raspberry-pi-02"
	clientToken    = "your-client-token"
	reconnectDelay = 5 * time.Second
)

func main() {
	client := NewClient(serverURL, clientID, clientToken)

	// 优雅退出处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		client.Stop()
	}()

	client.Run()
}
