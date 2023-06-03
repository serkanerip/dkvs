package dto

type PartitionTableDTO struct {
	Partitions map[int]string `msgpack:"partitions"`
}

type ClusterDTO struct {
	Nodes          []ClusterNodeDTO   `msgpack:"nodes"`
	PartitionTable *PartitionTableDTO `msgpack:"partition_table"`
	PartitionCount int                `msgpack:"partition_count"`
}

type ClusterNodeDTO struct {
	ID          string `msgpack:"id"`
	IP          string `msgpack:"ip"`
	ClientPort  string `msgpack:"client_port"`
	ClusterPort string `msgpack:"cluster_port"`
	StartTime   int64  `msgpack:"start_time"`
	Leader      bool   `msgpack:"leader"`
}
