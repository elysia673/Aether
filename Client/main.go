package main

import (
	"Aether/pkg/config"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "client.json", "path to config file")
	flag.Parse()

	cfg, err := config.LoadClient(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if cfg.UseHTTP {
		cfg.ServerURL = strings.Replace(cfg.ServerURL, "wss://", "ws://", 1)
	}

	client := NewClient(cfg.ServerURL, cfg.ClientID, cfg.ClientToken, cfg.UseHTTP, cfg.TLSSNI, cfg.Origin, time.Duration(cfg.ReconnectDelaySeconds)*time.Second)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		client.Stop()
	}()

	client.Run()
}
