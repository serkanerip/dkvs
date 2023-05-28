package tcp

type byteStream struct {
	b []byte
	i int
}

func (bs *byteStream) Next() []byte {
	b := bs.b[bs.i : bs.i+1]
	bs.i++
	return b
}

func (bs *byteStream) NextNBytes(n int) []byte {
	b := bs.b[bs.i : bs.i+n]
	bs.i += n
	return b
}

func (bs *byteStream) Remaining() []byte {
	return bs.b[bs.i:]
}

func (bs *byteStream) HasRemaining() bool {
	return bs.i != len(bs.b)
}
