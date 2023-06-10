package client

import (
	"dkvs/pkg"
	"dkvs/pkg/membership"
	"dkvs/pkg/message"
	"dkvs/pkg/partitiontable"
	"dkvs/pkg/tcp"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"net"
	"time"
)

type Client struct {
	address    string
	memberShip *membership.MemberShip
	pt         *partitiontable.PartitionTable
	connMap    map[string]*tcp.Connection
	packetCh   chan *tcp.Packet
}

func NewClient(address string) *Client {
	c := &Client{
		address:  address,
		connMap:  map[string]*tcp.Connection{},
		packetCh: make(chan *tcp.Packet),
	}
	conn, err := c.connectToServer(address, 0)
	if err != nil {
		panic(err)
	}
	c.connMap[address] = tcp.NewConnection(conn, c.packetCh, nil)
	c.getMembership()
	c.getPartitionTable()

	go c.handleTCPPackets()
	return c
}

func (c *Client) getMembership() {
	resp := c.sendMessageToAddress(&message.GetMemberShipQuery{}, c.address)
	m := &membership.MemberShip{}
	err := msgpack.Unmarshal(resp.Payload, m)
	if err != nil {
		panic(err)
	}
	c.updateMembership(m)
}

func (c *Client) getPartitionTable() {
	resp := c.sendMessageToAddress(&message.GetPartitionTableQuery{}, c.address)
	pt := &partitiontable.PartitionTable{}
	err := msgpack.Unmarshal(resp.Payload, pt)
	if err != nil {
		panic(err)
	}
	c.pt = pt
	fmt.Println("updated pt!")
}

func (c *Client) updateMembership(m *membership.MemberShip) {
	c.memberShip = m
	fmt.Println("membership updated!")
	fmt.Printf("LeaderID: %v\n", m.LeaderID)
	fmt.Printf("Node: %v\n", m.Nodes)
	c.handleMemberConnections()
}

func (c *Client) handleMemberConnections() {
	for _, node := range c.memberShip.Nodes {
		address := fmt.Sprintf("%s:%s", node.IP, node.ClientPort)
		if _, ok := c.connMap[address]; !ok {
			conn, err := c.connectToServer(address, 0)
			if err != nil {
				continue
			}
			c.connMap[address] = tcp.NewConnection(conn, c.packetCh, nil)
		}
	}
}

func (c *Client) connectToServer(addr string, retryCount int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("Couldn't connect to servers err is %v\n", err)
		if retryCount != 2 {
			time.Sleep(5 * time.Second)
			return c.connectToServer(addr, retryCount+1)
		}
	}
	return conn, err
}

func (c *Client) Get(key string) []byte {
	getOp := &message.GetOperation{Key: key}
	pid := pkg.GetPartitionIDByKey(23, []byte(key))
	pOwner := c.pt.Partitions[pid]
	return c.sendMessageToPartitionOwner(getOp, pOwner).Payload
}

func (c *Client) Put(key string, val []byte) {
	putOp := &message.PutOperation{Key: key, Value: val}
	pid := pkg.GetPartitionIDByKey(23, []byte(key))
	pOwner := c.pt.Partitions[pid]
	c.sendMessageToPartitionOwner(putOp, pOwner)
}

func (c *Client) sendMessageToAddress(msg message.Message, address string) *message.OperationResponse {
	conn := c.connMap[address]
	if conn == nil {
		panic(fmt.Sprintf("there is no open connection for address: %s\n", address))
	}
	resp, err := conn.Send(msg)
	if err != nil {
		panic(fmt.Sprintf("failed to write to %s\n", address))
	}
	return c.getOpResponseFromPacket(resp)
}

func (c *Client) sendMessageToPartitionOwner(msg message.Message, ownerId string) *message.OperationResponse {
	for _, n := range c.memberShip.Nodes {
		if n.ID == ownerId {
			return c.sendMessageToAddress(msg, fmt.Sprintf("%s:%s", n.IP, n.ClientPort))
		}
	}
	panic("couldn't find node by id!")
}

func (c *Client) getOpResponseFromPacket(packet *tcp.Packet) *message.OperationResponse {
	// fmt.Println("Received response's cid is:", packet.CorrelationId)

	if packet.MsgType == message.OPResponse {
		or := &message.OperationResponse{}
		err := msgpack.Unmarshal(packet.Body, or)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		return or
	}
	panic("unknown response msg type!")
}

func (c *Client) handleTCPPackets() {
	for packet := range c.packetCh {
		fmt.Println("New Packet received from server", packet.MsgType)
		if packet.MsgType == message.TopologyUpdatedE {
			msg := &membership.TopologyUpdatedEvent{}
			if err := msgpack.Unmarshal(packet.Body, msg); err != nil {
				panic(err)
			}
			c.updateMembership(msg.Membership)
		}
		response := &message.OperationResponse{
			IsSuccessful: "true",
			Error:        "",
			Payload:      nil,
		}
		fmt.Println(packet.MsgType)
		packet.Connection.SendAsyncWithCorrelationID(packet.CorrelationId, response)
		fmt.Println("response is sent")
	}
}
