package message

type OperationResponse struct {
	IsSuccessful string `json:"is_successful"`
	Error        string `json:"error"`
	Payload      []byte `json:"payload"`
}

func (o *OperationResponse) Type() MsgType {
	return OPResponse
}
