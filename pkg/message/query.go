package message

type GetMemberShipQuery struct {
}

func (p *GetMemberShipQuery) Type() MsgType {
	return GetMembershipQ
}

type GetPartitionTableQuery struct {
}

func (p *GetPartitionTableQuery) Type() MsgType {
	return GetPartitionTableQ
}
