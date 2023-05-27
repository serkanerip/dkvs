package partitiontable

import (
	"dkvs/common/dto"
	"fmt"
)

type PartitionTable struct {
	partitions  []*Partition
	localNodeID string
}

type Partition struct {
	ID       int
	OwnerID  string
	Assigned bool
}

func NewPartitionTable(count int, localNodeID string) *PartitionTable {
	pt := &PartitionTable{
		partitions: make([]*Partition, 0), localNodeID: localNodeID,
	}
	for i := 0; i < count; i++ {
		pt.partitions = append(pt.partitions, &Partition{
			ID:       i,
			OwnerID:  localNodeID,
			Assigned: true,
		})
	}
	return pt
}

func (pt *PartitionTable) GetOwnedPartitions() []int {
	ownedPartitions := make([]int, 0)
	for _, p := range pt.partitions {
		if p.OwnerID == pt.localNodeID {
			ownedPartitions = append(ownedPartitions, p.ID)
		}
	}
	return ownedPartitions
}

func (pt *PartitionTable) AssignTo(partitionCount int, ownerID string) {
	assignedPartitionCount := 0
	for _, p := range pt.partitions {
		if p.Assigned == true {
			fmt.Println("already assigned!")
			continue
		}
		if assignedPartitionCount == partitionCount {
			break
		}
		p.OwnerID = ownerID
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
		fmt.Printf("P%d Assigned: %v, Address: %s\n", p.ID, p.Assigned, p.OwnerID)
	}
}

func (pt *PartitionTable) ToDTO() *dto.PartitionTableDTO {
	d := &dto.PartitionTableDTO{Partitions: map[int]string{}}
	for _, p := range pt.partitions {
		d.Partitions[p.ID] = p.OwnerID
	}
	return d
}
