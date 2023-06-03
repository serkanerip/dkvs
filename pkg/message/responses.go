package message

type OperationResponse struct {
	IsSuccessful string `msgpack:"is_successful"`
	Error        string `msgpack:"error"`
	Payload      []byte `msgpack:"payload"`
}

func (o *OperationResponse) Type() MsgType {
	return OPResponse
}
