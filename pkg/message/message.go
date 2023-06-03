package message

type MsgType byte

const (
	ReadOP MsgType = iota
	PutOP
	OPResponse
	JoinOP
	GetPartitionTableQ
	GetClusterQ
	ClusterUpdatedE
)

func (m MsgType) String() string {
	return []string{
		"Read OP", "Put OP", "OP Response", "Join OP",
		"Get Partition Table Query", "Get Cluster Q", "Cluster Updated E",
	}[m]
}

type Message interface {
	Type() MsgType
}
