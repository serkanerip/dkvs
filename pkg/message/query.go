package message

type GetClusterQuery struct {
}

func (p *GetClusterQuery) Type() MsgType {
	return GetClusterQ
}
