package dto

type PartitionTableDTO struct {
	Partitions map[int]string `json:"partitions"`
}

type ClusterDTO struct {
	Nodes          []ClusterNodeDTO   `json:"nodes"`
	PartitionTable *PartitionTableDTO `json:"partition_table"`
	PartitionCount int                `json:"partition_count"`
}

type ClusterNodeDTO struct {
	ID          string `json:"id"`
	IP          string `json:"ip"`
	ClientPort  string `json:"client_port"`
	ClusterPort string `json:"cluster_port"`
	StartTime   int64  `json:"start_time"`
	Leader      bool   `json:"leader"`
}
