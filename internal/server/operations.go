package server

import (
	"dkvs/pkg/message"
)

type JoinOperation struct {
	ID          string `msgpack:"id"`
	IP          string `msgpack:"ip"`
	ClientPort  string `msgpack:"client_port"`
	ClusterPort string `msgpack:"cluster_port"`
	StartTime   int64  `msgpack:"start_time"`
	Leader      bool   `msgpack:"leader"`
}

func NewJoinOperation(n *Node) *JoinOperation {
	return &JoinOperation{
		ID:          n.config.ID.String(),
		IP:          n.config.IP,
		ClientPort:  n.config.ClientPort,
		ClusterPort: n.config.ClusterPort,
		StartTime:   n.startTime,
		Leader:      n.leader,
	}
}

func (p *JoinOperation) Type() message.MsgType {
	return message.JoinOP
}
