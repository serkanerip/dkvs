package partitiontable

import (
	"dkvs/pkg/message"
)

type UpdatedEvent struct {
	PartitionTable *PartitionTable
}

func (p *UpdatedEvent) Type() message.MsgType {
	return message.PTUpdatedE
}
