package main

import (
	"context"
	"dkvs/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	_, cancel := context.WithCancel(context.Background())
	s := server.NewServer(server.NewConfig())
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() { s.Start() }()
	defer func() {
		signal.Stop(quit)
		cancel()
		s.Close()
	}()
	<-quit
}
