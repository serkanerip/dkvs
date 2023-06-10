package server

import (
	"dkvs/pkg/membership"
	"dkvs/pkg/message"
	"dkvs/pkg/partitiontable"
	"dkvs/pkg/tcp"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
)

func (m *MembershipManager) Receive(p *tcp.Packet) {
	var payload []byte
	if p.MsgType == message.TopologyUpdatedE {
		fmt.Println("got topology updated event!")
		op := &membership.TopologyUpdatedEvent{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		go m.handleTopologyUpdatedE(op)
	}

	if p.MsgType == message.PTUpdatedE {
		fmt.Println("got pt updated event!")
		op := &partitiontable.UpdatedEvent{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		go m.handlePTUpdatedE(op)
	}

	if p.MsgType == message.GetMembershipQ {
		bytes, err := msgpack.Marshal(m.Membership)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = bytes
	}

	if p.MsgType == message.GetPartitionTableQ {
		bytes, err := msgpack.Marshal(m.pt)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = bytes
	}

	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      payload,
	}
	err := p.Connection.SendAsyncWithCorrelationID(p.CorrelationId, response)
	if err != nil {
		panic(fmt.Sprintf("couldn't send response, write failed %s!", err))
	}
}
