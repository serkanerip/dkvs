package main

import (
	"context"
	node "dkvs/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	n := node.NewNode(node.NewConfig(), ctx)
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
