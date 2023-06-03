package main

import (
	"context"
	"dkvs/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	n := server.NewNode(server.NewConfig())
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() { n.Start() }()
	defer func() {
		signal.Stop(quit)
		cancel()
		n.Close()
	}()
	<-quit
}
