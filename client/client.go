package client

import (
	"dkvs/common"
	"dkvs/common/dto"
	"dkvs/common/message"
	"dkvs/tcp"
	"encoding/json"
	"fmt"
	"net"
)

type Client struct {
	address  string
	cluster  *dto.ClusterDTO
	connMap  map[string]*tcp.Connection
	packetCh chan *tcp.Packet
}

func NewClient(address string) *Client {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		panic(fmt.Sprintf("Couldn't connect to servers err is %v\n", err))
	}

	c := &Client{
		address:  address,
		connMap:  map[string]*tcp.Connection{},
		packetCh: make(chan *tcp.Packet),
	}
	c.connMap[address] = tcp.NewConnection(conn, c.packetCh)
	c.getCluster()

	go c.handleTCPPackets()
	return c
}

func (c *Client) getCluster() {
	resp := c.sendMessageToAddress(&message.GetClusterQuery{}, c.address)
	cDTO := &dto.ClusterDTO{}
	err := json.Unmarshal(resp.Payload, cDTO)
	if err != nil {
		panic(err)
	}
	c.cluster = cDTO
}

func (c *Client) updateCluster(cluster *dto.ClusterDTO) {
	c.cluster = cluster
	fmt.Println("cluster details updated!")
	fmt.Printf("%v\n", cluster.PartitionTable)
	fmt.Printf("%v\n", cluster.Nodes)
	c.handleMemberConnections()
}

func (c *Client) handleMemberConnections() {
	for _, node := range c.cluster.Nodes {
		address := fmt.Sprintf("%s:%s", node.IP, node.ClientPort)
		if _, ok := c.connMap[address]; !ok {
			conn, err := net.Dial("tcp", address)
			if err != nil {
				panic(fmt.Sprintf("Couldn't connect to servers err is %v\n", err))
			}
			c.connMap[address] = tcp.NewConnection(conn, c.packetCh)
		}
	}
}

func (c *Client) Get(key string) []byte {
	getOp := &message.GetOperation{Key: key}
	pid := common.GetPartitionIDByKey(23, []byte(key))
	pOwner := c.cluster.PartitionTable.Partitions[pid]
	fmt.Println("pid", pid, "owner", pOwner)
	return c.sendMessageToPartitionOwner(getOp, pOwner).Payload
}

func (c *Client) Put(key string, val []byte) {
	putOp := &message.PutOperation{Key: key, Value: val}
	pid := common.GetPartitionIDByKey(23, []byte(key))
	pOwner := c.cluster.PartitionTable.Partitions[pid]
	fmt.Println("pid", pid, "owner", pOwner)
	c.sendMessageToPartitionOwner(putOp, pOwner)
}

func (c *Client) sendMessageToAddress(msg message.Message, address string) *message.OperationResponse {
	conn := c.connMap[address]
	return c.getOpResponseFromPacket(conn.Send(msg))
}

func (c *Client) sendMessageToPartitionOwner(msg message.Message, ownerId string) *message.OperationResponse {
	var node *dto.ClusterNodeDTO
	for _, nodeDTO := range c.cluster.Nodes {
		if nodeDTO.ID == ownerId {
			node = &nodeDTO
			break
		}
	}
	if node == nil {
		panic("couldn't find node by id!")
	}
	return c.sendMessageToAddress(msg, fmt.Sprintf("%s:%s", node.IP, node.ClientPort))
}

func (c *Client) getOpResponseFromPacket(packet *tcp.Packet) *message.OperationResponse {
	fmt.Println("Received response's cid is:", packet.CorrelationId)

	if packet.MsgType == message.OPResponse {
		or := &message.OperationResponse{}
		err := json.Unmarshal(packet.Body, or)
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
		if packet.MsgType == message.ClusterUpdatedE {
			msg := &message.ClusterUpdatedEvent{}
			if err := json.Unmarshal(packet.Body, msg); err != nil {
				panic(err)
			}
			c.updateCluster(msg.Cluster)
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
