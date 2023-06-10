package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
)

type Operation struct {
	ID          string `msgpack:"id"`
	IP          string `msgpack:"ip"`
	ClientPort  string `msgpack:"client_port"`
	ClusterPort string `msgpack:"cluster_port"`
	StartTime   int64  `msgpack:"start_time"`
}

func NewJoinOperation(n *node.Node) *Operation {
	return &Operation{
		ID:          n.ID,
		IP:          n.IP,
		ClientPort:  n.ClientPort,
		ClusterPort: n.ClusterPort,
		StartTime:   n.StartTime,
	}
}

func (p *Operation) Type() message.MsgType {
	return message.JoinOP
}

type OperationResponse struct {
	LeaderAddress string
}

func (p *OperationResponse) Type() message.MsgType {
	return message.JoinOPResp
}
