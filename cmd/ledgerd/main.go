package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"quorumledger/internal/api"
	"quorumledger/internal/consensus"
)

func main() {
	id := flag.String("id", "n1", "member ID in the cluster configuration")
	configPath := flag.String("config", "config/local.json", "cluster configuration")
	data := flag.String("data", "", "durable directory (default data/<id>)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "PaxosVault: A Resilient Distributed Banking and Settlement System Using Multi-Paxos\n\nUsage: ledgerd [flags]")
		flag.PrintDefaults()
	}
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	fail := func(err error) { log.Error("startup failed", "error", err); os.Exit(1) }
	cfg, err := consensus.LoadConfig(*configPath)
	if err != nil {
		fail(err)
	}
	if *data == "" {
		*data = filepath.Join("data", *id)
	}
	transport := api.NewTransport(cfg)
	node, err := consensus.New(cfg, *id, *data, transport)
	if err != nil {
		fail(err)
	}
	defer node.Close()
	u, _ := url.Parse(cfg.Nodes[*id])
	listener, err := net.Listen("tcp", u.Host)
	if err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Handler: api.NewServer(node, cfg).Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	runDone := make(chan struct{})
	go func() { defer close(runDone); node.Run(ctx) }()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		log.Info("member listening", "id", *id, "url", cfg.Nodes[*id], "data", *data)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server stopped", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "error", err)
		_ = server.Close()
	}
	<-runDone
	<-serverDone
	transport.Client.CloseIdleConnections()
	log.Info("member stopped", "id", *id)
}
