package server

import (
	"dkvs/common/message"
)

type JoinOperation struct {
	ID          string `json:"id"`
	IP          string `json:"ip"`
	ClientPort  string `json:"client_port"`
	ClusterPort string `json:"cluster_port"`
	StartTime   int64  `json:"start_time"`
	Leader      bool   `json:"leader"`
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
