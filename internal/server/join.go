package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/tcp"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"net"
	"sync"
	"time"
)

type JoinService struct {
	MemberShipManager *MembershipManager
	joinLock          sync.Mutex
	joinedToALeader   bool
}

func (js *JoinService) Join(memberAddresses []string) bool {
	localAddr := fmt.Sprintf("%s:%s", js.MemberShipManager.LocalNode.IP, js.MemberShipManager.LocalNode.ClusterPort)
	time.Sleep(2 * time.Second)

	for _, m := range memberAddresses {
		if m == localAddr {
			continue
		}
		joinErr := js.joinMember(m, 0)
		if joinErr != nil {
			fmt.Printf("join failed err is: %v\n", joinErr)
			continue
		}
		fmt.Println("joined to the member!")
		if js.joinedToALeader {
			break
		}
	}
	time.Sleep(time.Second * 1)
	js.joinLock.Lock()
	return js.joinedToALeader
}

func (js *JoinService) joinMember(memberAddr string, retryCount int) error {
	fmt.Println("Initializing a connection to:", memberAddr)
	conn, err := net.Dial("tcp", memberAddr)
	if err != nil {
		fmt.Println("Couldn't connect to:", memberAddr)
		if retryCount != 2 {
			return js.joinMember(memberAddr, retryCount+1)
		}
		return err
	}
	jo := NewJoinOperation(js.MemberShipManager.LocalNode)
	tcpConn := tcp.NewConnection(conn, nil, nil)
	respPacket, err := tcpConn.Send(jo)
	if err != nil {
		fmt.Println("Couldn't send join request err is", err)
		tcpConn.Close()
		return err
	}
	op := &message.OperationResponse{}
	err = msgpack.Unmarshal(respPacket.Body, op)
	if err != nil {
		return err
	}
	payload := &OperationResponse{}
	if err := msgpack.Unmarshal(op.Payload, payload); err != nil {
		return err
	}
	if op.Error == IAmNotTheLeader {
		return js.joinMember(payload.LeaderAddress, 0)
	}
	if payload.LeaderAddress == memberAddr {
		js.joinedToALeader = true
	}
	tcpConn.Close()
	return nil
}
