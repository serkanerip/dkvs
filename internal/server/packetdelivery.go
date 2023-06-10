package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/tcp"
	"fmt"
)

type PacketReceiver interface {
	Receive(packet *tcp.Packet)
}

type PacketDelivery struct {
	packetReceivers map[message.MsgType]PacketReceiver
}

func (ph PacketDelivery) Deliver(t *tcp.Packet) {
	if t.MsgType == message.OPResponse {
		fmt.Println("operation done!")
		return
	}
	receiver := ph.packetReceivers[t.MsgType]
	if receiver == nil {
		panic(fmt.Sprintf("Msg type(%s) doesn't have any reciever!\n", t.MsgType))
	}
	receiver.Receive(t)
}
