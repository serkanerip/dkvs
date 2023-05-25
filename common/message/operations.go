package message

type PutOperation struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (p *PutOperation) Type() MsgType {
	return PutOP
}

type GetOperation struct {
	Key string `json:"key"`
}

func (g GetOperation) Type() MsgType {
	return ReadOP
}
