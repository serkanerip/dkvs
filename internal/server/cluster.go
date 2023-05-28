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

func (c *Cluster) AddNode(newNode ClusterNode) {
	for _, node := range c.nodes {
		if node.id == newNode.id {
			fmt.Println("This node is already joined!")
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
	c.reassignPartitions()
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
	c.reassignPartitions()
}

func (c *Cluster) SetLocalNode(newNode ClusterNode) {
	c.localNode = newNode
	c.nodes = append(c.nodes, newNode)
	c.reassignPartitions()
}

func (c *Cluster) updateTable(table *dto.PartitionTableDTO) {
	c.pt.UnAssignAll()
	for i := 0; i < c.pCount; i++ {
		c.pt.AssignTo(i, table.Partitions[i])
	}
	c.pt.PrintPartitionTable()
}

func (c *Cluster) reassignPartitions() {
	fmt.Println("Reassigning partitions!")
	partitionPerNode := c.pCount / len(c.nodes)
	leftPartition := c.pCount - (partitionPerNode * len(c.nodes))
	c.pt.UnAssignAll()
	for i, node := range c.nodes {
		partitionCount := partitionPerNode
		if i == 0 {
			partitionPerNode += leftPartition
		}
		c.pt.AssignTo(partitionCount, node.id)
	}
	c.pt.PrintPartitionTable()
	fmt.Println("Partition Table Updated!")
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
