package server

import (
	"dkvs/internal/server/kvstore"
	"dkvs/internal/server/partitiontable"
	"dkvs/pkg/message"
	"dkvs/pkg/tcp"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"net"
	"os"
	"time"
)

type Node struct {
	startTime                 int64
	config                    *Config
	db                        *kvstore.KVStore
	leader                    bool
	clientListener            *tcp.Listener
	clusterListener           *tcp.Listener
	cluster                   Cluster
	clientPacketCh            chan *tcp.Packet
	clusterPacketCh           chan *tcp.Packet
	pendingClusterConnections map[string]*tcp.Connection
}

func NewNode(config *Config) *Node {
	cluster := Cluster{
		nodes:  []ClusterNode{},
		pt:     partitiontable.NewPartitionTable(config.PartitionCount, []string{}),
		pCount: config.PartitionCount,
	}
	db := kvstore.NewKVStore(config.PartitionCount)
	n := &Node{
		startTime:                 time.Now().UnixNano(),
		db:                        db,
		cluster:                   cluster,
		clusterPacketCh:           make(chan *tcp.Packet),
		clientPacketCh:            make(chan *tcp.Packet),
		config:                    config,
		pendingClusterConnections: map[string]*tcp.Connection{},
	}
	return n
}

func (n *Node) Start() {
	fmt.Printf("Node is starting with [id=%s,ip=%s]\n", n.config.ID, n.config.IP)

	n.cluster.onPartitionTableUpdate = n.sendPartitionTableToReplicasAndClients
	n.cluster.SetLocalNode(ClusterNode{
		id:          n.config.ID.String(),
		ip:          n.config.IP,
		clientPort:  n.config.ClientPort,
		clusterPort: n.config.ClusterPort,
		leader:      n.leader,
		conn:        nil,
		startTime:   n.startTime,
	})

	n.clusterListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.config.IP, n.config.ClusterPort),
		PacketCh: n.clusterPacketCh,
	}
	if err := n.clusterListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clusterPacketCh, 1)

	n.joinMembers()

	n.clientListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.config.IP, n.config.ClientPort),
		PacketCh: n.clientPacketCh,
	}
	if err := n.clientListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clientPacketCh, 5)

	fmt.Println("Node is started!")
}

func (n *Node) sendPartitionTableToReplicasAndClients() {
	if !n.leader {
		return
	}

	go func() {
		if n.clientListener == nil {
			return
		}
		for _, clientConn := range n.clientListener.Connections {
			clientConn.Send(&message.ClusterUpdatedEvent{Cluster: n.cluster.ToDTO()})
		}
		fmt.Println("Shared latest cluster details with clients!")
	}()
}

func (n *Node) startPacketHandlerWorkers(ch chan *tcp.Packet, workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for packet := range ch {
				n.handleTCPPacket(packet)
			}
		}()
	}
}

func (n *Node) joinMembers() {
	if len(n.config.MemberList) != 0 {
		for _, m := range n.config.MemberList {
			_, joinErr := n.joinMember(m, 0)
			if joinErr != nil {
				fmt.Printf("join failed err is: %v\n", joinErr)
			}
		}
		return
	}
	time.Sleep(5 * time.Second)
	ips, err := net.LookupIP("dkvs-headless-svc.default.svc.cluster.local")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not get IPs: %v\n", err)
		return
	}
	for _, ip := range ips {
		fmt.Printf("IN A %s\n", ip.String())
		if ip.String() != n.config.IP {
			_, joinErr := n.joinMember(fmt.Sprintf("%s:6060", ip.String()), 0)
			if joinErr != nil {
				fmt.Printf("join failed err is: %v\n", joinErr)
			}
		}
	}
}

func (n *Node) joinMember(memberAddr string, retryCount int) (*tcp.Connection, error) {
	fmt.Println("joining to", memberAddr)
	conn, err := net.Dial("tcp", memberAddr)
	if err != nil {
		if retryCount != 2 {
			return n.joinMember(memberAddr, retryCount+1)
		}
		return nil, err
	}
	jo := NewJoinOperation(n)
	tcpConn := tcp.NewConnection(conn, n.clusterPacketCh, nil)
	respPacket, err := tcpConn.Send(jo)
	if err != nil {
		tcpConn.Close()
		return nil, err
	}
	op := &message.OperationResponse{}
	err = msgpack.Unmarshal(respPacket.Body, op)
	if err != nil {
		panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
	}
	if op.Error == AlreadyJoinedErr {
		tcpConn.Close()
		return nil, errors.New(AlreadyJoinedErr)
	}
	n.pendingClusterConnections[memberAddr] = tcpConn
	fmt.Println("got join response!")
	return tcpConn, nil
}

func (n *Node) Close() {
	n.clientListener.Close()
	n.clusterListener.Close()
}

func (n *Node) handleTCPPacket(t *tcp.Packet) {
	// fmt.Println("Received packet's cid is:", t.CorrelationId)

	var payload []byte
	if t.MsgType == message.OPResponse {
		fmt.Println("operation done!")
		return
	}

	if t.MsgType == message.JoinOP {
		n.handleJoinOP(t)
		return
	}

	if t.MsgType == message.ReadOP {
		op := &message.GetOperation{}
		err := msgpack.Unmarshal(t.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = n.db.Get(op.Key)
	}

	if t.MsgType == message.PutOP {
		op := &message.PutOperation{}
		err := msgpack.Unmarshal(t.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		n.db.Put(op.Key, op.Value)
	}

	if t.MsgType == message.GetPartitionTableQ {
		ptDTO := n.cluster.pt.ToDTO()
		bytes, err := msgpack.Marshal(ptDTO)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = bytes
	}

	if t.MsgType == message.GetClusterQ {
		cDTO := n.cluster.ToDTO()
		bytes, err := msgpack.Marshal(cDTO)
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
	err := t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	if err != nil {
		panic(fmt.Sprintf("couldn't send response, write failed %s!", err))
	}
}

func (n *Node) handleJoinOP(t *tcp.Packet) {
	op := &JoinOperation{}
	err := msgpack.Unmarshal(t.Body, op)
	if err != nil {
		panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
	}
	fmt.Printf(
		"[Type]: %s,[id]: %s, [IP]: %s, [Leader]: %v\n",
		t.MsgType, op.ID, op.IP, op.Leader,
	)
	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      nil,
	}
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	fmt.Println("join response is sent")
	if n.cluster.HasNode(op.ID) {
		fmt.Printf("This node has already joined to node(%s)!\n", op.ID)
		return
	}
	tcpConn := n.pendingClusterConnections[fmt.Sprintf("%s:%s", op.IP, op.ClusterPort)]
	if tcpConn == nil { // if this node hasn't joined to the cluster which sent the join request
		tcpConn, err = n.joinMember(fmt.Sprintf("%s:%s", op.IP, op.ClusterPort), 0)
		if err != nil {
			fmt.Printf("join failed err is: %v\n", err)
		}
	} else {
		delete(n.pendingClusterConnections, fmt.Sprintf("%s:%s", op.IP, op.ClusterPort))
	}
	n.cluster.AddNode(ClusterNode{
		id:          op.ID,
		ip:          op.IP,
		clientPort:  op.ClientPort,
		clusterPort: op.ClusterPort,
		leader:      op.Leader,
		conn:        tcpConn,
		startTime:   op.StartTime,
	})
	fmt.Println("join done!")
}
