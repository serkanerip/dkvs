package server

import (
	"dkvs/pkg/membership"
	"dkvs/pkg/message"
	"dkvs/pkg/node"
	"dkvs/pkg/partitiontable"
	"dkvs/pkg/tcp"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"net"
	"sort"
	"time"
)

type MembershipManager struct {
	LocalNode              *node.Node
	Membership             *membership.MemberShip
	pt                     *partitiontable.PartitionTable
	leaderNode             *node.Node
	connections            map[string]*tcp.Connection
	PartitionCount         int
	onPartitionTableUpdate func()
	allowParticipation     bool
}

func NewMemberShipManager(pCount int) *MembershipManager {
	return &MembershipManager{
		LocalNode: nil,
		Membership: &membership.MemberShip{
			Nodes:    []*node.Node{},
			LeaderID: "",
		},
		pt:                     nil,
		leaderNode:             nil,
		connections:            map[string]*tcp.Connection{},
		PartitionCount:         pCount,
		onPartitionTableUpdate: nil,
		allowParticipation:     true,
	}
}

func (m *MembershipManager) Start() {
	m.allowParticipation = false
	defer func() { m.allowParticipation = true }()
	var leaderNode *node.Node
	for i := range m.Membership.Nodes {
		if leaderNode == nil {
			leaderNode = m.Membership.Nodes[i]
			continue
		}
		if leaderNode.StartTime > m.Membership.Nodes[i].StartTime {
			leaderNode = m.Membership.Nodes[i]
		}
	}
	m.SetNewLeader(leaderNode)
	if m.AmILeader() {
		m.fireTopologyUpdatedEvent()
		time.Sleep(50 * time.Millisecond)
		if m.pt == nil {
			fmt.Println("PT hasn't been created, creating it!")
			m.pt = partitiontable.NewPartitionTable(m.PartitionCount, m.Membership.NodeIds())
			m.pt.PrintPartitionTable()
		}
		m.firePartitionTableUpdatedEvent()
	}
}

func (m *MembershipManager) LeaderAddr() string {
	if !m.HasLeader() {
		return ""
	}
	return fmt.Sprintf("%s:%s", m.leaderNode.IP, m.leaderNode.ClusterPort)
}

func (m *MembershipManager) HasLeader() bool {
	return m.leaderNode != nil
}

func (m *MembershipManager) AmILeader() bool {
	if !m.HasLeader() {
		return false
	}
	return m.LocalNode.ID == m.leaderNode.ID
}

func (m *MembershipManager) HasNode(id string) bool {
	for _, n := range m.Membership.Nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func (m *MembershipManager) AddNode(newNode *node.Node) error {
	for _, n := range m.Membership.Nodes {
		if n.ID == newNode.ID {
			fmt.Println("This node is already joined!")
			return nil
		}
	}
	if !m.allowParticipation {
		return errors.New(ParticipationIsNotAllowed)
	}
	if _, ok := m.connections[newNode.ID]; !ok {
		if err := m.initializeConnectionTo(newNode); err != nil {
			return err
		}
	}
	m.Membership.Nodes = append(m.Membership.Nodes, newNode)
	sort.Slice(m.Membership.Nodes, func(i, j int) bool {
		return m.Membership.Nodes[i].StartTime < m.Membership.Nodes[j].StartTime
	})
	fmt.Println("New node added!", m.Membership.Nodes)
	computeLost := false
	if newNode.StartTime > m.LocalNode.StartTime {
		computeLost = true
	}
	if m.AmILeader() {
		go func() {
			m.fireTopologyUpdatedEvent()
			m.reassignPartitions(computeLost)
		}()
	}
	return nil
}

func (m *MembershipManager) fireTopologyUpdatedEvent() {
	fmt.Println("sharing new membership")
	for _, n := range m.Membership.Nodes {
		if n.ID == m.LocalNode.ID {
			continue
		}
		_, err := m.connections[n.ID].Send(&membership.UpdatedEvent{Membership: m.Membership})
		if err != nil {
			fmt.Println("sending topology updated event failed err is", err)
			return
		}
	}
}

func (m *MembershipManager) firePartitionTableUpdatedEvent() {
	fmt.Println("firing pt updated event!")
	time.Sleep(1 * time.Second)
	for _, n := range m.Membership.Nodes {
		if n.ID == m.LocalNode.ID {
			continue
		}
		_, err := m.connections[n.ID].Send(&partitiontable.UpdatedEvent{PartitionTable: m.pt})
		if err != nil {
			fmt.Println("sending pt updated event failed err is", err)
			return
		}
	}
}

func (m *MembershipManager) broadcastMessage(msg message.Message) {
	for _, n := range m.Membership.Nodes {
		if n.ID == m.LocalNode.ID {
			continue
		}
		conn := m.connections[n.ID]
		if conn.IsClosed() {
			continue
		}
		_, err := conn.Send(msg)
		if err != nil {
			fmt.Println("couldn't broadcast the msg, err is", err)
			return
		}
	}
}

func (m *MembershipManager) SetNewLeader(n *node.Node) {
	m.leaderNode = n
	m.Membership.LeaderID = n.ID
	fmt.Println("[INFO] New leader is", n.ID)
}

func (m *MembershipManager) initializeConnectionTo(n *node.Node) error {
	memberAddr := fmt.Sprintf("%s:%s", n.IP, n.ClusterPort)
	fmt.Println("Initializing a new connection to", memberAddr)
	conn, err := net.Dial("tcp", memberAddr)
	if err != nil {
		fmt.Println("[INFO] Couldn't initialize a connection to", memberAddr)
		return err
	}
	m.connections[n.ID] = tcp.NewConnection(conn, nil, func() {
		m.onNodeConnectionDrop(n.ID)
	})
	fmt.Println("Initialized connection to", memberAddr)
	return nil
}

func (m *MembershipManager) getNodeByID(nodeId string) *node.Node {
	for _, n := range m.Membership.Nodes {
		if n.ID == nodeId {
			return n
		}
	}
	return nil
}

func (m *MembershipManager) nextLeader() *node.Node {
	var ln *node.Node
	for _, n := range m.Membership.Nodes {
		if m.HasLeader() && m.leaderNode.ID == n.ID {
			fmt.Println("is leader", n.ID)
			continue
		}
		if n.ID != m.LocalNode.ID {
			if _, ok := m.connections[n.ID]; !ok {
				fmt.Println("no conn", n.ID)
				continue
			}
			if m.connections[n.ID].IsClosed() {
				fmt.Println("c closed", n.ID)
				continue
			}
		}
		if ln == nil {
			fmt.Println("ln nil")
			ln = n
			continue
		}
		fmt.Printf("[%s:%d] - [%s:%d]", ln.ID, ln.StartTime, n.ID, n.StartTime)
		if ln.StartTime > n.StartTime {
			ln = n
		}
	}
	return ln
}

func (m *MembershipManager) onNodeConnectionDrop(nodeId string) {
	m.allowParticipation = false
	defer func() { m.allowParticipation = true }()
	fmt.Printf("Lost connection to node(%s)!\n", nodeId)

	n := m.getNodeByID(nodeId)
	if n == nil {
		fmt.Println("[WARN] Node disconnected and it's not in membership, removing connection!")
		delete(m.connections, nodeId)
		return
	}
	fmt.Println("[INFO] Retrying to initialize a connection!")
	time.Sleep(200 * time.Millisecond)
	err := m.initializeConnectionTo(n)
	if err == nil {
		fmt.Println("[INFO] Connection recovered!")
		return
	}
	if m.leaderNode.ID == nodeId {
		m.leaderLeaveTheMembership()
	}
	if m.AmILeader() {
		m.removeNode(nodeId)
	}
}

func (m *MembershipManager) leaderLeaveTheMembership() {
	nextLeader := m.nextLeader()
	m.leaderNode = nil
	t := time.NewTicker(1 * time.Second)
	fmt.Println("[WARN] Leader node gone! Waiting new leader to be determinate!")
	fmt.Println("[INFO] Next leader id is", nextLeader.ID)
	for range t.C {
		if m.leaderNode != nil {
			break
		}
		fmt.Println("[WARN] Leader node gone! Waiting new leader to be determinate!")
		fmt.Println("[INFO] Next leader id is", nextLeader.ID)
		if nextLeader.ID == m.LocalNode.ID {
			m.SetNewLeader(m.LocalNode)
			t.Stop()
			break
		}
	}
}

func (m *MembershipManager) removeNode(nodeId string) {
	fmt.Printf("Removing node(%s) from membership!\n", nodeId)
	var newList []*node.Node
	for _, n := range m.Membership.Nodes {
		if n.ID != nodeId {
			newList = append(newList, n)
		}
	}
	m.Membership.Nodes = newList
	delete(m.connections, nodeId)
	m.fireTopologyUpdatedEvent()
	m.reassignPartitions(true)
}

func (m *MembershipManager) SetLocalNode(newNode *node.Node) {
	m.LocalNode = newNode
	m.Membership.Nodes = append(m.Membership.Nodes, newNode)
	m.reassignPartitions(false)
}

func (m *MembershipManager) handleMMUpdatedEvent(e *membership.UpdatedEvent) {
	fmt.Println("[INFO] Handling membership updated event!")
	e.Membership.PrettyPrint()
	if m.leaderNode != nil && m.leaderNode.ID != e.Membership.LeaderID {
		fmt.Println("[WARN] LeaderID in event doesn't match with known leader id!")
		return
	}
	m.Membership = e.Membership
	fmt.Println("[INFO] Updated membership!")
	for _, n := range e.Membership.Nodes {
		if n.ID == m.LocalNode.ID {
			continue
		}
		if n.ID == m.Membership.LeaderID {
			m.SetNewLeader(n)
		}
		if _, ok := m.connections[n.ID]; !ok {
			err := m.initializeConnectionTo(n)
			if err != nil {
				// Todo handle
			}
		}
	}
	for id, _ := range m.connections {
		if m.HasNode(id) {
			continue
		}
		if !m.connections[id].IsClosed() {
			m.connections[id].Close()
		}
		delete(m.connections, id)
	}
}

func (m *MembershipManager) handlePTUpdatedE(e *partitiontable.UpdatedEvent) {
	fmt.Println("[INFO] Handling pt updated event!")
	if m.AmILeader() {
		fmt.Println("[WARN] I am the leader, I don't accept partition table!")
		return
	}
	m.pt = e.PartitionTable
	fmt.Println("[INFO] Updated partition table!")
}

func (m *MembershipManager) reassignPartitions(computeLost bool) {
	if m.pt == nil {
		fmt.Println("[WARN] Partition table hasn't been created yet!")
		if m.AmILeader() {
			fmt.Println("[INFO] Creating the partition table!")
			m.pt = partitiontable.NewPartitionTable(m.PartitionCount, m.Membership.NodeIds())
			m.pt.PrintPartitionTable()
		}
		fmt.Println("no reassignment will happen cuz pt hasn't been created yet!")
		return
	}
	fmt.Println("Recreating partitions table!")
	nextPt := m.pt.CreateWithNewOwners(m.Membership.NodeIds())
	if computeLost {
		fmt.Printf("Lost partitions: %v\n", m.pt.ComputeLostPartitionsOf(nextPt, m.LocalNode.ID))
	}
	m.pt = nextPt
	m.pt.PrintPartitionTable()
	fmt.Println("New partition table created!")
	m.firePartitionTableUpdatedEvent()
}

func (m *MembershipManager) Receive(p *tcp.Packet) {
	var payload []byte
	var errMsg string
	isSuccessful := "true"
	if p.MsgType == message.MembershipUpdatedE {
		op := &membership.UpdatedEvent{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		go m.handleMMUpdatedEvent(op)
	}

	if p.MsgType == message.PTUpdatedE {
		op := &partitiontable.UpdatedEvent{}
		err := msgpack.Unmarshal(p.Body, op)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		go m.handlePTUpdatedE(op)
	}

	if p.MsgType == message.GetMembershipQ {
		bytes, err := msgpack.Marshal(m.Membership)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = bytes
	}

	if p.MsgType == message.GetPartitionTableQ {
		bytes, err := msgpack.Marshal(m.pt)
		if err != nil {
			panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
		}
		payload = bytes
	}

	response := &message.OperationResponse{
		IsSuccessful: isSuccessful,
		Error:        errMsg,
		Payload:      payload,
	}
	err := p.Connection.SendAsyncWithCorrelationID(p.CorrelationId, response)
	if err != nil {
		panic(fmt.Sprintf("couldn't send response, write failed %s!", err))
	}
}
