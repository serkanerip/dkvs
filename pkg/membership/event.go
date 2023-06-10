package membership

import "dkvs/pkg/message"

type TopologyUpdatedEvent struct {
	Membership *MemberShip `msgpack:"membership"`
}

func (p *TopologyUpdatedEvent) Type() message.MsgType {
	return message.TopologyUpdatedE
}
