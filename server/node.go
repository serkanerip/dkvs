package server

import (
	"context"
	"dkvs/common/dto"
	"dkvs/common/message"
	"dkvs/server/kvstore"
	"dkvs/server/leader_election"
	"dkvs/server/partitiontable"
	"dkvs/tcp"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net"
)

type Node struct {
	id                 uuid.UUID
	db                 *kvstore.KVStore
	ip                 string
	clientPort         string
	clusterPort        string
	le                 *leader_election.LeaderElection
	leader             bool
	clientListener     *tcp.Listener
	clusterListener    *tcp.Listener
	knownMemberAddress string
	cluster            Cluster
	clientPacketCh     chan *tcp.Packet
	clusterPacketCh    chan *tcp.Packet
}

func NewNode(clientPort, clusterPort, knownMember, etcdAddress string) *Node {
	id := uuid.New()
	ip := GetOutboundIP().String()
	address := fmt.Sprintf("%s:%s", ip, clientPort)
	partitionCount := 23
	pt := partitiontable.NewPartitionTable(partitionCount, address)
	cluster := Cluster{
		nodes:  []ClusterNode{},
		pt:     pt,
		pCount: partitionCount,
	}
	db := kvstore.NewKVStore(partitionCount, pt.GetOwnedPartitions()...)
	n := &Node{
		id:                 id,
		db:                 db,
		ip:                 ip,
		clientPort:         clientPort,
		clusterPort:        clusterPort,
		le:                 leader_election.NewLeaderElection(context.Background(), etcdAddress),
		knownMemberAddress: knownMember,
		cluster:            cluster,
		clientPacketCh:     make(chan *tcp.Packet),
	}
	return n
}

func (n *Node) Start() {
	fmt.Printf("Node is starting with [id=%s,ip=%s]\n", n.id, n.ip)

	// elect
	leaderID := n.le.Elect(n.id.String())
	if leaderID == n.id.String() {
		n.leader = true
		log.Println("This instance is the leader!")
	} else {
		log.Println("This instance is replica!")
	}

	n.cluster.AddNode(ClusterNode{
		id:          n.id.String(),
		ip:          n.ip,
		clientPort:  n.clientPort,
		clusterPort: n.clusterPort,
		leader:      n.leader,
		conn:        nil,
	})

	n.clusterListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.ip, n.clusterPort),
		PacketCh: n.clientPacketCh,
	}
	if err := n.clusterListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clusterPacketCh, 1)

	if !n.leader {
		//n.join()
	}

	n.clientListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.ip, n.clientPort),
		PacketCh: n.clientPacketCh,
	}
	if err := n.clientListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clientPacketCh, 5)

	fmt.Println("Node is started!")
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

func (n *Node) join() {
	fmt.Println("joining to", n.knownMemberAddress)
	// TODO implement JOIN OP
	//conn, err := net.Dial("tcp", n.knownMemberAddress)
	//if err != nil {
	//	panic(fmt.Sprintf("Couldn't connect to servers err is %v\n", err))
	//}
	//jo := NewJoinOperation(n)
	//_, err = conn.Write(common.Serialize(jo))
	//if err != nil {
	//	panic(err)
	//}
	////go n.handleConnection(conn)
	fmt.Println("join done")
}

func (n *Node) Close() {
	n.le.Close()
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
		op := &JoinOperation{}
		err := json.Unmarshal(t.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		fmt.Printf(
			"[Type]: %s,[id]: %s, [IP]: %s, [Leader]: %v\n",
			t.MsgType, op.ID, op.IP, op.Leader,
		)
		n.cluster.AddNode(ClusterNode{
			id:          op.ID,
			ip:          op.IP,
			clientPort:  op.ClientPort,
			clusterPort: op.ClusterPort,
			leader:      op.Leader,
			conn:        t.Connection,
		})
		nDTO := dto.ClusterNodeDTO{
			ID:          n.id.String(),
			IP:          n.ip,
			ClientPort:  n.clientPort,
			ClusterPort: n.clusterPort,
			Leader:      n.leader,
		}
		bytes, err := json.Marshal(nDTO)
		if err != nil {
			panic(fmt.Sprintf("marshalling failed err is :%v\n", err))
		}
		payload = bytes
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

	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      payload,
	}
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
	fmt.Println("response is sent")

}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
