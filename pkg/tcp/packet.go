package tcp

import (
	"dkvs/pkg/message"
)

type Packet struct {
	TotalLen      uint64
	MsgType       message.MsgType
	CorrelationId string
	Body          []byte
	Connection    *Connection
}

func PacketFromRaw(conn *Connection, buf []byte) *Packet {
	bs := byteStream{b: buf}
	_ = bs.NextNBytes(8) // consume totalLen
	msgTypeBytes := bs.NextNBytes(1)
	msgType := message.MsgType(msgTypeBytes[0])
	cid := string(bs.NextNBytes(36))

	return &Packet{
		TotalLen:      uint64(len(buf)),
		MsgType:       msgType,
		CorrelationId: cid,
		Body:          bs.Remaining(),
		Connection:    conn,
	}
}
