package server

import (
	"dkvs/common/message"
	"github.com/google/uuid"
)

type JoinOperation struct {
	ID          string `json:"id"`
	IP          string `json:"ip"`
	ClientPort  string `json:"client_port"`
	ClusterPort string `json:"cluster_port"`
	Leader      bool   `json:"leader"`
	CID         string `json:"CID"`
}

func NewJoinOperation(n *Node) *JoinOperation {
	return &JoinOperation{
		ID:          n.id.String(),
		IP:          n.ip,
		ClientPort:  n.clientPort,
		ClusterPort: n.clusterPort,
		Leader:      n.leader,
		CID:         uuid.New().String(),
	}
}

func (p *JoinOperation) CorrelationID() string {
	return p.CID
}

func (p *JoinOperation) Type() message.MsgType {
	return message.JoinOP
}
