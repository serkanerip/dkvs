package message

type MsgType byte

const (
	ReadOP MsgType = iota
	PutOP
	JoinOP
	GetMembershipQ
	GetPartitionTableQ
	MembershipUpdatedE
	PTUpdatedE
	JoinOPResp
	OPResponse
)

func (m MsgType) String() string {
	return []string{
		"Read OP", "Put OP", "Join OP",
		"Get Membership Query", "GetPartitionTableQ",
		"MM Updated Event", "PT Updated Event",
		"Join OP Response", "OP Response",
	}[m]
}

type Message interface {
	Type() MsgType
}
