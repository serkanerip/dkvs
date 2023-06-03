package partitiontable

import (
	"dkvs/pkg/dto"
	"fmt"
	"strings"
	"sync"
)

type PartitionTable struct {
	mu         sync.Mutex
	partitions []*Partition
}

type Partition struct {
	ID       int
	OwnerID  string
	Assigned bool
}

func NewPartitionTable(count int, owners []string) *PartitionTable {
	pt := &PartitionTable{
		partitions: make([]*Partition, 0),
	}
	for i := 0; i < count; i++ {
		pt.partitions = append(pt.partitions, &Partition{
			ID:       i,
			OwnerID:  "",
			Assigned: false,
		})
	}
	if len(owners) != 0 {
		pt.repartition(owners)
	}
	return pt
}

func (pt *PartitionTable) repartition(ids []string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	partitionPerNode := len(pt.partitions) / len(ids)
	idIndex := 0
	for i := range pt.partitions {
		if i != 0 && i%partitionPerNode == 0 {
			idIndex = (idIndex + 1) % len(ids)
		}
		pt.partitions[i].Assigned = true
		pt.partitions[i].OwnerID = ids[idIndex]
	}
}

func (pt *PartitionTable) ComputeLostPartitionsOf(nextPt *PartitionTable, owner string) []int {
	var lostPartitions []int
	for i := range pt.partitions {
		if pt.partitions[i].OwnerID != owner {
			continue
		}
		if nextPt.partitions[i].OwnerID != pt.partitions[i].OwnerID {
			lostPartitions = append(lostPartitions, i)
		}
	}
	return lostPartitions
}

func (pt *PartitionTable) PrintPartitionTable() {
	m := map[string][]string{}
	for _, p := range pt.partitions {
		if _, ok := m[p.OwnerID]; !ok {
			m[p.OwnerID] = make([]string, 0)
		}
		m[p.OwnerID] = append(m[p.OwnerID], fmt.Sprintf("%d", p.ID))
	}
	for k, v := range m {
		fmt.Printf("[%s]'s partitions: %s\n", k, strings.Join(v, ","))
	}
}

func (pt *PartitionTable) ToDTO() *dto.PartitionTableDTO {
	d := &dto.PartitionTableDTO{Partitions: map[int]string{}}
	for _, p := range pt.partitions {
		d.Partitions[p.ID] = p.OwnerID
	}
	return d
}
