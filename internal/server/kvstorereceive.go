package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/tcp"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
)

func (k *KVStore) Receive(p *tcp.Packet) {
	var payload []byte
	if p.MsgType == message.ReadOP {
		op := &message.GetOperation{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = k.Get(op.Key)
	}

	if p.MsgType == message.PutOP {
		op := &message.PutOperation{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		k.Put(op.Key, op.Value)
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
