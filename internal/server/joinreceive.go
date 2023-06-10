package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
	"dkvs/pkg/tcp"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
)

func (js *JoinService) Receive(p *tcp.Packet) {
	if p.MsgType == message.JoinOP {
		js.handleJoinOP(p)
		return
	}
}

func (js *JoinService) handleJoinOP(t *tcp.Packet) {
	js.joinLock.Lock()
	defer js.joinLock.Unlock()
	op := &Operation{}
	err := msgpack.Unmarshal(t.Body, op)
	if err != nil {
		panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
	}
	fmt.Printf(
		"[Type]: %s,[id]: %s, [IP]: %s\n",
		t.MsgType, op.ID, op.IP,
	)
	respPayload := OperationResponse{LeaderAddress: js.MemberShipManager.LeaderAddr()}
	respPayloadBytes, marshallErr := msgpack.Marshal(respPayload)
	if !js.MemberShipManager.AmILeader() && js.MemberShipManager.HasLeader() {
		fmt.Println("im not the leader sending leader addr!")
		if marshallErr != nil {
			panic("couldn't marshall join resp payload!")
		}
		response := &message.OperationResponse{
			IsSuccessful: "false",
			Error:        IAmNotTheLeader,
			Payload:      respPayloadBytes,
		}
		t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
		return
	}
	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      respPayloadBytes,
	}
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	js.MemberShipManager.AddNode(&node.Node{
		ID:          op.ID,
		IP:          op.IP,
		ClientPort:  op.ClientPort,
		ClusterPort: op.ClusterPort,
		StartTime:   op.StartTime,
	})
	fmt.Println("new node joined successfully!")
}
