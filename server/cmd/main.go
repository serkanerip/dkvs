package main

import (
	node "dkvs/server"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	knownMember := os.Getenv("MEMBER")
	clientPort := envOrDefault("CLIENT_PORT", "6050")
	clusterPort := envOrDefault("CLUSTER_PORT", "6060")
	etcdAddr := envOrDefault("ETCD_ADDR", "localhost:2379")
	fmt.Println(etcdAddr)
	n := node.NewNode(clientPort, clusterPort, knownMember, etcdAddr)
	defer n.Close()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() { n.Start() }()
	<-quit
}

func envOrDefault(envName, def string) string {
	val, ok := os.LookupEnv(envName)
	if !ok {
		val = def
	}
	return val
}
