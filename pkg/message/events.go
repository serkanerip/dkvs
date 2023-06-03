package message

import (
	"dkvs/pkg/dto"
)

type ClusterUpdatedEvent struct {
	Cluster *dto.ClusterDTO `json:"cluster"`
}

func (p *ClusterUpdatedEvent) Type() MsgType {
	return ClusterUpdatedE
}
