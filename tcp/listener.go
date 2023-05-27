package tcp

import (
	"fmt"
	"net"
)

type Listener struct {
	Addr        string
	listener    net.Listener
	Connections []*Connection
	PacketCh    chan *Packet
}

func (t *Listener) Start() error {
	var err error
	t.listener, err = net.Listen("tcp", t.Addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening for requests on", t.Addr)
	go t.acceptConnections()
	return err
}

func (t *Listener) Close() error {
	fmt.Println("Closing listener!")
	return t.listener.Close()
}

func (t *Listener) acceptConnections() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("Closing listener, connection acception failed err is:%v\n", err)
			return
		}
		fmt.Println("Accepted new connection", conn.RemoteAddr())
		t.Connections = append(t.Connections, NewConnection(conn, t.PacketCh))
	}
}
