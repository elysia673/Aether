package main

import (
	"Aether/common/config"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func printVersion() {
	fmt.Printf("Aether Client %s (%s) %s\n", Version, GitCommit, BuildTime)
}

func main() {
	configPath := flag.String("config", "client.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		printVersion()
		return
	}

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
		log.Println("收到关闭信号")
		client.Stop()
	}()

	client.Run()
}
