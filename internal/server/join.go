package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
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

func (js *JoinService) Join(memberAddresses []string) *node.Node {
	localAddr := fmt.Sprintf("%s:%s", js.MemberShipManager.LocalNode.IP, js.MemberShipManager.LocalNode.ClusterPort)
	time.Sleep(2 * time.Second)
	var leaderNode *node.Node

	for _, m := range memberAddresses {
		if m == localAddr {
			continue
		}
		var joinErr error
		leaderNode, joinErr = js.joinMember(m, 0)
		if joinErr != nil {
			fmt.Printf("join failed err is: %v\n", joinErr)
			continue
		}
		fmt.Println("joined to the member!")
		if leaderNode != nil {
			break
		}
	}
	time.Sleep(time.Second * 1)
	js.joinLock.Lock()
	return leaderNode
}

func (js *JoinService) joinMember(memberAddr string, retryCount int) (*node.Node, error) {
	fmt.Println("Initializing a connection to:", memberAddr)
	conn, err := net.Dial("tcp", memberAddr)
	if err != nil {
		fmt.Println("Couldn't connect to:", memberAddr)
		if retryCount != 2 {
			time.Sleep(200 * time.Millisecond)
			return js.joinMember(memberAddr, retryCount+1)
		}
		return nil, err
	}
	jo := NewJoinOperation(js.MemberShipManager.LocalNode)
	tcpConn := tcp.NewConnection(conn, nil, nil)
	respPacket, err := tcpConn.Send(jo)
	if err != nil {
		fmt.Println("Couldn't send join request err is", err)
		tcpConn.Close()
		return nil, err
	}
	op := &message.OperationResponse{}
	err = msgpack.Unmarshal(respPacket.Body, op)
	if err != nil {
		return nil, err
	}
	payload := &OperationResponse{}
	if err := msgpack.Unmarshal(op.Payload, payload); err != nil {
		return nil, err
	}
	if op.Error == IAmNotTheLeader {
		return js.joinMember(payload.Leader.GetClusterAddr(), 0)
	}
	if op.Error == ParticipationIsNotAllowed {
		fmt.Println("Node didn't allow for participating")
		if retryCount != 2 {
			fmt.Println("Retrying...")
			time.Sleep(200 * time.Millisecond)
			return js.joinMember(memberAddr, retryCount+1)
		}
	}
	tcpConn.Close()
	return payload.Leader, nil
}

func (js *JoinService) Receive(p *tcp.Packet) {
	if p.MsgType == message.JoinOP {
		js.handleJoinOP(p)
		return
	}
}

func (js *JoinService) handleJoinOP(t *tcp.Packet) {
	js.joinLock.Lock()
	defer js.joinLock.Unlock()
	op := &Operation{}
	err := msgpack.Unmarshal(t.Body, op)
	if err != nil {
		panic(fmt.Sprintf("unmarshalling failed err is :%v\n", err))
	}
	fmt.Printf(
		"[Type]: %s,[id]: %s, [IP]: %s\n",
		t.MsgType, op.ID, op.IP,
	)
	respPayload := OperationResponse{Leader: js.MemberShipManager.leaderNode}
	respPayloadBytes, marshallErr := msgpack.Marshal(respPayload)
	if !js.MemberShipManager.AmILeader() && js.MemberShipManager.HasLeader() {
		fmt.Println("im not the leader sending leader addr!")
		if marshallErr != nil {
			panic("couldn't marshall join resp payload!")
		}
		response := &message.OperationResponse{
			IsSuccessful: "false",
			Error:        IAmNotTheLeader,
			Payload:      respPayloadBytes,
		}
		t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
		return
	}
	response := &message.OperationResponse{
		IsSuccessful: "true",
		Error:        "",
		Payload:      respPayloadBytes,
	}
	err = js.MemberShipManager.AddNode(&node.Node{
		ID:          op.ID,
		IP:          op.IP,
		ClientPort:  op.ClientPort,
		ClusterPort: op.ClusterPort,
		StartTime:   op.StartTime,
	})
	if err != nil {
		response.IsSuccessful = "false"
		response.Error = err.Error()
	}
	t.Connection.SendAsyncWithCorrelationID(t.CorrelationId, response)
}
