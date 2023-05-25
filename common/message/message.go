package message

type MsgType byte

const (
	ReadOP MsgType = iota
	PutOP
	OPResponse
	JoinOP
	GetPartitionTableQ
	GetClusterQ
)

func (m MsgType) String() string {
	return []string{"Read OP", "Put OP", "OP Response", "Join OP", "Get Partition Table Query", "Get Cluster Q"}[m]
}

type Message interface {
	Type() MsgType
}
