package tcp

import (
	"dkvs/common/message"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net"
	"time"
)

type Connection struct {
	conn              net.Conn
	PacketCh          chan *Packet
	packetSubscribers map[string]chan *Packet
}

func NewConnection(conn net.Conn, ch chan *Packet) *Connection {
	c := &Connection{
		conn:              conn,
		PacketCh:          ch,
		packetSubscribers: map[string]chan *Packet{},
	}
	go c.read()
	return c
}

func (c *Connection) Send(msg message.Message) *Packet {
	cid := uuid.New().String()
	return c.send(cid, msg)
}

func (c *Connection) SendAsync(msg message.Message) *Packet {
	cid := uuid.New().String()
	return c.send(cid, msg)
}

func (c *Connection) SendAsyncWithCorrelationID(cid string, msg message.Message) {
	c.sendAsync(cid, msg)
}

func (c *Connection) SendWithCorrelationID(cid string, msg message.Message) *Packet {
	return c.send(cid, msg)
}

func (c *Connection) send(cid string, msg message.Message) *Packet {
	c.conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	b := c.serializeMsg(cid, msg)
	ch := make(chan *Packet)
	c.packetSubscribers[cid] = ch
	_, err := c.conn.Write(b)
	if err != nil {
		panic(fmt.Sprintf("couldn't write to conn err is: %v\n", err))
	}
	fmt.Println("Message send waiting for a response!")
	return <-ch
}

func (c *Connection) sendAsync(cid string, msg message.Message) {
	b := c.serializeMsg(cid, msg)
	_, err := c.conn.Write(b)
	if err != nil {
		panic(fmt.Sprintf("couldn't write to conn err is: %v\n", err))
	}
	fmt.Println("Message send async!")
}

func (c *Connection) serializeMsg(cid string, msg message.Message) []byte {
	b, err := json.Marshal(msg)
	if err != nil {
		panic("couldn't serialize msg")
	}
	ttl := MessageHeaderLength + len(b)

	// total length
	buffer := make([]byte, 0, ttl)
	buffer = binary.BigEndian.AppendUint64(buffer, uint64(ttl))

	// msg type
	buffer = append(buffer, byte(msg.Type()))

	// correlation id
	buffer = append(buffer, []byte(cid)...)
	buffer = append(buffer, b...)
	return buffer
}

func (c *Connection) read() {
	// TODO remove connection in listener's connection list on close
	defer c.conn.Close()
	var msgBuffer []byte
	var totalLenBuffer []byte
	var err error
	for {
		buffer := make([]byte, 4096)
		var readByteCount int
		readByteCount, err = c.conn.Read(buffer)
		if err != nil {
			break
		}
		bufferStream := byteStream{b: buffer[:readByteCount]}
		for bufferStream.HasRemaining() {
			b := bufferStream.Next()[0]
			if totalLenBuffer == nil {
				totalLenBuffer = make([]byte, 0, 8)
			}

			if len(totalLenBuffer) != cap(totalLenBuffer) {
				totalLenBuffer = append(totalLenBuffer, b)
				continue
			}

			totalLen := binary.BigEndian.Uint64(totalLenBuffer)
			// fmt.Println("message total len is", totalLen)

			if msgBuffer == nil {
				msgBuffer = make([]byte, 0, totalLen)
				msgBuffer = append(msgBuffer, totalLenBuffer...)
			}

			// fmt.Println("op buffer len is", len(msgBuffer))
			if len(msgBuffer) != cap(msgBuffer) {
				msgBuffer = append(msgBuffer, b)
			}

			if (len(msgBuffer)) != cap(msgBuffer) {
				continue
			}

			packet := PacketFromRaw(c, msgBuffer)
			s, ok := c.packetSubscribers[packet.CorrelationId]
			if ok {
				s <- packet
			} else {
				c.PacketCh <- PacketFromRaw(c, msgBuffer)
			}

			msgBuffer = nil
			totalLenBuffer = nil
		}
	}
	fmt.Printf("Failed to read from connection(%s) err is:%v! Closing connection!\n",
		c.conn.RemoteAddr(), err)
}
