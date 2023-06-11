package tcp

import (
	"dkvs/pkg/message"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn              net.Conn
	PacketCh          chan *Packet
	packetSubscribers sync.Map
	OnClose           func()
	closed            bool
}

func NewConnection(conn net.Conn, ch chan *Packet, onClose func()) *Connection {
	c := &Connection{
		conn:              conn,
		PacketCh:          ch,
		packetSubscribers: sync.Map{},
		OnClose:           onClose,
	}
	go c.read()
	return c
}

func (c *Connection) Close() {
	if c.OnClose != nil {
		c.OnClose()
	}
	c.conn.Close()
	c.closed = true
}

func (c *Connection) IsClosed() bool {
	return c.closed
}

func (c *Connection) Send(msg message.Message) (*Packet, error) {
	cid := uuid.New().String()
	return c.send(cid, msg)
}

func (c *Connection) SendAsync(msg message.Message) error {
	cid := uuid.New().String()
	return c.SendAsyncWithCorrelationID(cid, msg)
}

func (c *Connection) SendAsyncWithCorrelationID(cid string, msg message.Message) error {
	return c.sendAsync(cid, msg)
}

func (c *Connection) SendWithCorrelationID(cid string, msg message.Message) (*Packet, error) {
	return c.send(cid, msg)
}

func (c *Connection) send(cid string, msg message.Message) (*Packet, error) {
	c.conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	var data []byte
	if msg != nil {
		data = c.serializeMsg(cid, msg)
	}
	ch := make(chan *Packet)
	c.packetSubscribers.Store(cid, ch)
	_, err := c.conn.Write(data)
	if err != nil {
		return nil, err
	}
	return <-ch, nil
}

func (c *Connection) sendAsync(cid string, msg message.Message) error {
	c.conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	var data []byte
	if msg != nil {
		data = c.serializeMsg(cid, msg)
	}
	_, err := c.conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) serializeMsg(cid string, msg message.Message) []byte {
	b, err := msgpack.Marshal(&msg)
	if err != nil {
		panic(fmt.Sprintf("couldn't serialize msg err is: %v\n", err))
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
	defer c.conn.Close()
	var msgBuffer []byte
	var totalLenBuffer []byte
	var totalLen uint64
	var err error
	for {
		buffer := make([]byte, 4096)
		var readByteCount int
		readByteCount, err = c.conn.Read(buffer)
		if err != nil {
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				break
			}
			fmt.Printf("Failed to read from connection(%s) err is:%v\n", c.conn.RemoteAddr(), err)
			break
		}
		bufferStream := byteStream{b: buffer[:readByteCount]}
		for bufferStream.HasRemaining() {
			if totalLenBuffer == nil {
				totalLenBuffer = make([]byte, 0, 8)
			}

			if len(totalLenBuffer) != cap(totalLenBuffer) {
				bc := int(math.Min(
					float64(cap(totalLenBuffer)-len(totalLenBuffer)),
					float64(bufferStream.RemainingBytesCount()),
				))
				totalLenBuffer = append(totalLenBuffer, bufferStream.NextNBytes(bc)...)
				continue
			} else {
				totalLen = binary.BigEndian.Uint64(totalLenBuffer)
			}

			if msgBuffer == nil {
				msgBuffer = make([]byte, 0, totalLen)
				msgBuffer = append(msgBuffer, totalLenBuffer...)
			}

			if len(msgBuffer) != cap(msgBuffer) {
				bc := int(math.Min(
					float64(cap(msgBuffer)-len(msgBuffer)),
					float64(bufferStream.RemainingBytesCount()),
				))
				msgBuffer = append(msgBuffer, bufferStream.NextNBytes(bc)...)
			}

			if (len(msgBuffer)) != cap(msgBuffer) {
				continue
			}

			packet := PacketFromRaw(c, msgBuffer)
			s, ok := c.packetSubscribers.Load(packet.CorrelationId)
			if ok {
				s.(chan *Packet) <- packet
			} else {
				if c.PacketCh != nil {
					c.PacketCh <- packet
				} else {
					fmt.Println("Got a new packet but there is no reciever channel!")
				}
			}

			msgBuffer = nil
			totalLenBuffer = nil
		}
	}

	fmt.Printf("Closing connection to %s!\n", c.conn.RemoteAddr())
	if !errors.Is(err, net.ErrClosed) {
		c.Close()
	}
}
