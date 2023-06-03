package message

type PutOperation struct {
	Key   string `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

func (p *PutOperation) Type() MsgType {
	return PutOP
}

type GetOperation struct {
	Key string `msgpack:"key"`
}

func (g GetOperation) Type() MsgType {
	return ReadOP
}
