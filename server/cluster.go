package server

import (
	"dkvs/common/dto"
	"dkvs/server/partitiontable"
	"dkvs/tcp"
	"fmt"
)

type OnRepartitioning func()

type Cluster struct {
	nodes                   []ClusterNode
	pt                      *partitiontable.PartitionTable
	pCount                  int
	repartitioningListeners []OnRepartitioning
}

type ClusterNode struct {
	id          string
	ip          string
	clientPort  string
	clusterPort string
	leader      bool
	conn        *tcp.Connection
}

func (c *Cluster) AddNode(newNode ClusterNode) {
	for _, node := range c.nodes {
		if node.id == newNode.id {
			fmt.Println("This node is already joined!")
		}
	}
	c.nodes = append(c.nodes, newNode)
	fmt.Println("New node added!")
	c.repartition()
}

func (c *Cluster) repartition() {
	fmt.Println("Repartitioning")
	partitionPerNode := c.pCount / len(c.nodes)
	leftPartition := c.pCount - (partitionPerNode * len(c.nodes))
	c.pt.UnAssignAll()
	for i, node := range c.nodes {
		fmt.Println("node", node)
		partitionCount := partitionPerNode
		if i == 0 {
			partitionPerNode += leftPartition
		}
		address := fmt.Sprintf("%s:%s", node.ip, node.clientPort)
		c.pt.AssignTo(partitionCount, address)
	}
	c.pt.PrintPartitionTable()
	if c.repartitioningListeners != nil {
		for _, listener := range c.repartitioningListeners {
			listener()
		}
	}
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
