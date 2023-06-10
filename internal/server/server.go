package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
	"fmt"
	"time"
)

type Server struct {
	c  *Config
	pd *PacketDelivery
	ne *NodeEngine
	mm *MembershipManager
	kv *KVStore
	js *JoinService
}

func NewServer(c *Config) *Server {
	s := &Server{c: c}
	s.pd = &PacketDelivery{packetReceivers: map[message.MsgType]PacketReceiver{}}
	lc := &node.Node{
		ID:          c.ID.String(),
		IP:          c.IP,
		ClientPort:  c.ClientPort,
		ClusterPort: c.ClusterPort,
		StartTime:   time.Now().UnixNano(),
	}
	s.mm = NewMemberShipManager(c.PartitionCount)
	s.mm.SetLocalNode(lc)
	s.kv = NewKVStore(c.PartitionCount)
	s.js = &JoinService{MemberShipManager: s.mm}
	s.ne = NewNodeEngine(s.mm, s.kv, s.js, c, s.pd)
	s.assignPacketReceivers()

	return s
}

func (s *Server) assignPacketReceivers() {
	s.pd.packetReceivers[message.JoinOP] = s.js
	s.pd.packetReceivers[message.JoinOPResp] = s.js
	s.pd.packetReceivers[message.TopologyUpdatedE] = s.mm
	s.pd.packetReceivers[message.PTUpdatedE] = s.mm
	s.pd.packetReceivers[message.GetMembershipQ] = s.mm
	s.pd.packetReceivers[message.GetPartitionTableQ] = s.mm
	s.pd.packetReceivers[message.ReadOP] = s.kv
	s.pd.packetReceivers[message.PutOP] = s.kv
}

func (s *Server) Start() {
	fmt.Printf("Server is starting with [id=%s,ip=%s]\n", s.c.ID, s.c.IP)
	s.ne.Start()
}

func (s *Server) Close() {
	s.ne.Close()
}
