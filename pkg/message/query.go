package message

type GetPartitionTableQuery struct {
}

func (p *GetPartitionTableQuery) Type() MsgType {
	return GetPartitionTableQ
}

type GetClusterQuery struct {
}

func (p *GetClusterQuery) Type() MsgType {
	return GetClusterQ
}
