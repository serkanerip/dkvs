package server

import (
	"dkvs/pkg/membership"
	"dkvs/pkg/node"
	"dkvs/pkg/partitiontable"
	"dkvs/pkg/tcp"
	"fmt"
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
	}
}

func (m *MembershipManager) DetermineLeader() {
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
	m.leaderNode = leaderNode
	m.Membership.LeaderID = m.leaderNode.ID
	fmt.Println("leader is", leaderNode.ID)
	if m.AmILeader() {
		m.fireTopologyUpdatedEvent()
		time.Sleep(50 * time.Millisecond)
		if m.pt == nil {
			fmt.Println("PT hasn't been created, creating it!")
			var ids []string
			for _, n := range m.Membership.Nodes {
				ids = append(ids, n.ID)
			}
			m.pt = partitiontable.NewPartitionTable(m.PartitionCount, ids)
			m.pt.PrintPartitionTable()
		}
		m.firePartitionTableUpdatedEvent()
	}
}

func (m *MembershipManager) LeaderAddr() string {
	if m.leaderNode == nil {
		return ""
	}
	return fmt.Sprintf("%s:%s", m.leaderNode.IP, m.leaderNode.ClusterPort)
}

func (m *MembershipManager) HasLeader() bool {
	return m.leaderNode != nil
}

func (m *MembershipManager) AmILeader() bool {
	if m.leaderNode == nil {
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

func (m *MembershipManager) AddNode(newNode *node.Node) {
	for _, n := range m.Membership.Nodes {
		if n.ID == newNode.ID {
			fmt.Println("This node is already joined!")
			return
		}
	}
	if _, ok := m.connections[newNode.ID]; !ok {
		memberAddr := fmt.Sprintf("%s:%s", newNode.IP, newNode.ClusterPort)
		fmt.Println("Initializing a new connection to", memberAddr)
		conn, err := net.Dial("tcp", memberAddr)
		if err != nil {
			panic(fmt.Sprintf("couldn't connect to the new added node err is %v!\n", err))
		}
		m.connections[newNode.ID] = tcp.NewConnection(conn, nil, func() {
			m.onNodeLeft(newNode.ID)
		})
		fmt.Println("Initialized connection to", memberAddr)
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
}

func (m *MembershipManager) fireTopologyUpdatedEvent() {
	fmt.Println("sharing new membership")
	for _, n := range m.Membership.Nodes {
		if n.ID == m.LocalNode.ID {
			continue
		}
		_, err := m.connections[n.ID].Send(&membership.TopologyUpdatedEvent{Membership: m.Membership})
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

func (m *MembershipManager) onNodeLeft(nodeId string) {
	fmt.Printf("NodeEngine[%s] left the mm, deleting from the list!\n", nodeId)
	var newList []*node.Node
	for _, n := range m.Membership.Nodes {
		if n.ID != nodeId {
			newList = append(newList, n)
		}
	}
	m.Membership.Nodes = newList
	m.reassignPartitions(true)
}

func (m *MembershipManager) SetLocalNode(newNode *node.Node) {
	m.LocalNode = newNode
	m.Membership.Nodes = append(m.Membership.Nodes, newNode)
	m.reassignPartitions(false)
}

func (m *MembershipManager) handleTopologyUpdatedE(e *membership.TopologyUpdatedEvent) {
	fmt.Println("[INFO] Handling topology updated event!")
	fmt.Printf("%+v\n", e.Membership)
	if m.leaderNode != nil && m.leaderNode.ID != e.Membership.LeaderID {
		fmt.Println("[WARN] LeaderID in event doesn't match with known leader id!")
		return
	}
	m.Membership = e.Membership
	fmt.Println("[INFO] Updated membership!")
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
		fmt.Println("no reassignment will happen cuz pt hasn't been created yet!")
		return
	}
	fmt.Println("Recreating partitions table!")
	var ids []string
	for _, n := range m.Membership.Nodes {
		ids = append(ids, n.ID)
	}
	nextPt := m.pt.CreateWithNewOwners(ids)
	if computeLost {
		fmt.Printf("Lost partitions: %v\n", m.pt.ComputeLostPartitionsOf(nextPt, m.LocalNode.ID))
	}
	m.pt = nextPt
	m.pt.PrintPartitionTable()
	fmt.Println("New partition table created!")
	if m.onPartitionTableUpdate != nil {
		go m.onPartitionTableUpdate()
	}
}
