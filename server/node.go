package server

import (
	"context"
	"dkvs/common/message"
	"dkvs/server/kvstore"
	"dkvs/server/leader_election"
	"dkvs/server/partitiontable"
	"dkvs/tcp"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

type Node struct {
	startTime       int64
	config          *Config
	db              *kvstore.KVStore
	le              *leader_election.LeaderElection
	leader          bool
	clientListener  *tcp.Listener
	clusterListener *tcp.Listener
	cluster         Cluster
	clientPacketCh  chan *tcp.Packet
	clusterPacketCh chan *tcp.Packet
}

func NewNode(config *Config, ctx context.Context) *Node {
	address := fmt.Sprintf("%s:%s", config.IP, config.ClientPort)
	pt := partitiontable.NewPartitionTable(
		config.PartitionCount,
		address,
	)
	cluster := Cluster{
		nodes:        []ClusterNode{},
		joiningNodes: []ClusterNode{},
		pt:           pt,
		pCount:       config.PartitionCount,
	}
	db := kvstore.NewKVStore(config.PartitionCount, pt.GetOwnedPartitions()...)
	n := &Node{
		startTime:       time.Now().UnixNano(),
		db:              db,
		le:              leader_election.NewLeaderElection(ctx, config.ETCDAddress),
		cluster:         cluster,
		clusterPacketCh: make(chan *tcp.Packet),
		clientPacketCh:  make(chan *tcp.Packet),
		config:          config,
	}
	return n
}

func (n *Node) Start() {
	fmt.Printf("Node is starting with [id=%s,ip=%s]\n", n.config.ID, n.config.IP)

	// elect
	leaderID := n.le.Elect(n.config.ID.String())
	if leaderID == n.config.ID.String() {
		n.leader = true
		log.Println("This instance is the leader!")
	} else {
		log.Println("This instance is replica!")
	}

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

	n.joinMembers()
	n.startPacketHandlerWorkers(n.clusterPacketCh, 1)

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
	if n.leader == false {
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
	for _, memberAddr := range n.config.MemberList {
		fmt.Println("joining to", memberAddr)
		conn, err := net.Dial("tcp", memberAddr)
		if err != nil {
			panic(fmt.Sprintf("Couldn't connect to servers err is %v\n", err))
		}
		jo := NewJoinOperation(n)
		tcpConn := tcp.NewConnection(conn, n.clusterPacketCh)
		tcpConn.Send(jo)
		fmt.Println("got join response!")
		receivedPacket := <-n.clusterPacketCh
		if receivedPacket.MsgType != message.JoinOP {
			panic("received packet must be join op!")
		}
		n.handleJoinOP(receivedPacket, tcpConn)
		fmt.Println("joined to", memberAddr)
	}
}

func (n *Node) Close() {
	n.clientListener.Close()
	n.clusterListener.Close()
}

func (n *Node) handleTCPPacket(t *tcp.Packet) {
	fmt.Println("Received packet's cid is:", t.CorrelationId)

	var payload []byte
	if t.MsgType == message.OPResponse {
		fmt.Println("operation done!")
		return
	}

	if t.MsgType == message.JoinOP {
		n.handleJoinOP(t, nil)
		return
	}

	if t.MsgType == message.ReadOP {
		op := &message.GetOperation{}
		err := json.Unmarshal(t.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		fmt.Printf("[Type]: %s,[Key]: %s\n", t.MsgType, op.Key)
		payload = n.db.Get(op.Key)
	}

	if t.MsgType == message.PutOP {
		op := &message.PutOperation{}
		err := json.Unmarshal(t.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		n.db.Put(op.Key, op.Value)
		fmt.Printf("[Type]: %b,[Key]: %s, [Val]: %s\n", t.MsgType, op.Key, string(op.Value))
	}

	if t.MsgType == message.GetPartitionTableQ {
		ptDTO := n.cluster.pt.ToDTO()
		bytes, err := json.Marshal(ptDTO)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		fmt.Printf("[Type]: %s\n", t.MsgType)
		payload = bytes
	}

	if t.MsgType == message.GetClusterQ {
		cDTO := n.cluster.ToDTO()
		bytes, err := json.Marshal(cDTO)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		fmt.Printf("[Type]: %s\n", t.MsgType)
		payload = bytes
	}

	if t.MsgType == message.PartitionTableUpdatedE {
		e := &message.PartitionTableUpdatedEvent{}
		if err := json.Unmarshal(t.Body, e); err != nil {
			panic(err)
		}
		n.cluster.updateTable(e.Table)
		fmt.Printf("[Type]: %s\n", t.MsgType)
	}

	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      payload,
	}
	fmt.Println(t.MsgType)
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	fmt.Println("response is sent")
}

func (n *Node) handleJoinOP(t *tcp.Packet, tcpConn *tcp.Connection) {
	op := &JoinOperation{}
	err := json.Unmarshal(t.Body, op)
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
	fmt.Println(t.MsgType)
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	fmt.Println("join response is sent")
	if tcpConn == nil {
		fmt.Printf("joining to %s:%s\n", op.IP, op.ClusterPort)
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", op.IP, op.ClusterPort))
		if err != nil {
			panic(fmt.Sprintf("Couldn't connect to servers err is %v\n", err))
		}
		jo := NewJoinOperation(n)
		tcpConn = tcp.NewConnection(conn, n.clusterPacketCh)
		tcpConn.Send(jo)
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
