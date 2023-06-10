package partitiontable

import (
	"fmt"
	"strings"
)

type PartitionTable struct {
	Partitions map[int]string
	pCount     int
}

func NewPartitionTable(count int, owners []string) *PartitionTable {
	pt := &PartitionTable{
		pCount:     count,
		Partitions: map[int]string{},
	}
	for i := 0; i < count; i++ {
		pt.Partitions[i] = ""
	}
	if len(owners) != 0 {
		pt.repartition(owners)
	}
	return pt
}

func (pt *PartitionTable) CreateWithNewOwners(ids []string) *PartitionTable {
	return NewPartitionTable(pt.pCount, ids)
}

func (pt *PartitionTable) repartition(ids []string) {
	partitionPerNode := len(pt.Partitions) / len(ids)
	idIndex := 0
	for i := range pt.Partitions {
		if i != 0 && i%partitionPerNode == 0 {
			idIndex = (idIndex + 1) % len(ids)
		}
		pt.Partitions[i] = ids[idIndex]
	}
}

func (pt *PartitionTable) ComputeLostPartitionsOf(nextPt *PartitionTable, owner string) []int {
	var lostPartitions []int
	for i := range pt.Partitions {
		if pt.Partitions[i] != owner {
			continue
		}
		if nextPt.Partitions[i] != pt.Partitions[i] {
			lostPartitions = append(lostPartitions, i)
		}
	}
	return lostPartitions
}

func (pt *PartitionTable) PrintPartitionTable() {
	m := map[string][]string{}
	for p, ownerID := range pt.Partitions {
		if _, ok := m[ownerID]; !ok {
			m[ownerID] = make([]string, 0)
		}
		m[ownerID] = append(m[ownerID], fmt.Sprintf("%d", p))
	}
	for k, v := range m {
		fmt.Printf("[%s]'s Partitions: %s\n", k, strings.Join(v, ","))
	}
}
