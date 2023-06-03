package server

import (
	"dkvs/internal/server/partitiontable"
	"dkvs/pkg/dto"
	"dkvs/pkg/tcp"
	"fmt"
	"sort"
)

type Cluster struct {
	localNode              ClusterNode
	nodes                  []ClusterNode
	pt                     *partitiontable.PartitionTable
	pCount                 int
	onPartitionTableUpdate func()
}

type ClusterNode struct {
	id          string
	ip          string
	clientPort  string
	clusterPort string
	leader      bool
	conn        *tcp.Connection
	startTime   int64
}

func (c *Cluster) HasNode(id string) bool {
	for _, node := range c.nodes {
		if node.id == id {
			return true
		}
	}
	return false
}

func (c *Cluster) AddNode(newNode ClusterNode) {
	for _, node := range c.nodes {
		if node.id == newNode.id {
			fmt.Println("This node is already joined!")
			return
		}
	}
	newNode.conn.OnClose = func() {
		c.onNodeLeft(newNode.id)
	}
	c.nodes = append(c.nodes, newNode)
	sort.Slice(c.nodes, func(i, j int) bool {
		return c.nodes[i].startTime < c.nodes[j].startTime
	})
	fmt.Println("New node added!")
	computeLost := false
	if newNode.startTime > c.localNode.startTime {
		computeLost = true
	}
	c.reassignPartitions(computeLost)
}

func (c *Cluster) onNodeLeft(nodeId string) {
	fmt.Printf("Node[%s] left the cluster, deleting from the list!\n", nodeId)
	var newList []ClusterNode
	for _, node := range c.nodes {
		if node.id != nodeId {
			newList = append(newList, node)
		}
	}
	c.nodes = newList
	c.reassignPartitions(true)
}

func (c *Cluster) SetLocalNode(newNode ClusterNode) {
	c.localNode = newNode
	c.nodes = append(c.nodes, newNode)
	c.reassignPartitions(false)
}

func (c *Cluster) reassignPartitions(computeLost bool) {
	fmt.Println("Recreating partitions table!")
	var ids []string
	for _, node := range c.nodes {
		ids = append(ids, node.id)
	}
	nextPt := partitiontable.NewPartitionTable(c.pCount, ids)
	if computeLost {
		fmt.Printf("Lost partitions: %v\n", c.pt.ComputeLostPartitionsOf(nextPt, c.localNode.id))
	}
	c.pt = nextPt
	c.pt.PrintPartitionTable()
	fmt.Println("New partition table created!")
	go c.onPartitionTableUpdate()
}

func (c *Cluster) ToDTO() *dto.ClusterDTO {
	d := &dto.ClusterDTO{
		Nodes:          []dto.ClusterNodeDTO{},
		PartitionTable: c.pt.ToDTO(),
		PartitionCount: c.pCount,
	}

	for _, node := range c.nodes {
		d.Nodes = append(d.Nodes, dto.ClusterNodeDTO{
			ID:          node.id,
			IP:          node.ip,
			ClientPort:  node.clientPort,
			ClusterPort: node.clusterPort,
			Leader:      node.leader,
		})
	}

	return d
}
