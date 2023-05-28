package message

type MsgType byte

const (
	ReadOP MsgType = iota
	PutOP
	OPResponse
	JoinOP
	GetPartitionTableQ
	GetClusterQ
	PartitionTableUpdatedE
	ClusterUpdatedE
)

func (m MsgType) String() string {
	return []string{
		"Read OP", "Put OP", "OP Response", "Join OP", "Get Partition Table Query", "Get Cluster Q",
		"Partition Table Updated E", "Cluster Updated E", "Join Partitions Table Req",
	}[m]
}

type Message interface {
	Type() MsgType
}
