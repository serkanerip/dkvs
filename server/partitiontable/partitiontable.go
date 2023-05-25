package partitiontable

import (
	"dkvs/common/dto"
	"fmt"
)

type PartitionTable struct {
	partitions  []*Partition
	nodeAddress string
}

type Partition struct {
	ID          int
	NodeAddress string
	Assigned    bool
}

func NewPartitionTable(count int, address string) *PartitionTable {
	pt := &PartitionTable{
		partitions: make([]*Partition, 0), nodeAddress: address,
	}
	for i := 0; i < count; i++ {
		pt.partitions = append(pt.partitions, &Partition{
			ID:          i,
			NodeAddress: address,
			Assigned:    true,
		})
	}
	return pt
}

func (pt *PartitionTable) GetOwnedPartitions() []int {
	ownedPartitions := make([]int, 0)
	for _, p := range pt.partitions {
		if p.NodeAddress == pt.nodeAddress {
			ownedPartitions = append(ownedPartitions, p.ID)
		}
	}
	return ownedPartitions
}

func (pt *PartitionTable) AssignTo(partitionCount int, address string) {
	assignedPartitionCount := 0
	for _, p := range pt.partitions {
		if p.Assigned == true {
			fmt.Println("already assigned!")
			continue
		}
		if assignedPartitionCount == partitionCount {
			break
		}
		p.NodeAddress = address
		p.Assigned = true
		assignedPartitionCount++
	}
}

func (pt *PartitionTable) UnAssignAll() {
	for _, p := range pt.partitions {
		p.Assigned = false
	}
}

func (pt *PartitionTable) PrintPartitionTable() {
	for _, p := range pt.partitions {
		fmt.Printf("P%d Assigned: %v, Address: %s\n", p.ID, p.Assigned, p.NodeAddress)
	}
}

func (pt *PartitionTable) ToDTO() *dto.PartitionTableDTO {
	d := &dto.PartitionTableDTO{Partitions: map[int]string{}}
	for _, p := range pt.partitions {
		d.Partitions[p.ID] = p.NodeAddress
	}
	return d
}
