package node

import "fmt"

type Node struct {
	ID          string
	IP          string
	ClientPort  string
	ClusterPort string
	StartTime   int64
	State       string
}

func (n *Node) GetClusterAddr() string {
	return fmt.Sprintf("%s:%s", n.IP, n.ClusterPort)
}
