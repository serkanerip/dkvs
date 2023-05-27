package message

import (
	"dkvs/common/dto"
)

type PartitionTableUpdatedEvent struct {
	Table *dto.PartitionTableDTO `json:"table"`
}

func (p *PartitionTableUpdatedEvent) Type() MsgType {
	return PartitionTableUpdatedE
}

type ClusterUpdatedEvent struct {
	Cluster *dto.ClusterDTO `json:"cluster"`
}

func (p *ClusterUpdatedEvent) Type() MsgType {
	return ClusterUpdatedE
}
