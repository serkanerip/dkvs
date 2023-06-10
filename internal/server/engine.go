package server

import (
	"dkvs/pkg/membership"
	"dkvs/pkg/tcp"
	"fmt"
)

type NodeEngine struct {
	config          *Config
	db              *KVStore
	clientListener  *tcp.Listener
	clusterListener *tcp.Listener
	mm              *MembershipManager
	clientPacketCh  chan *tcp.Packet
	clusterPacketCh chan *tcp.Packet
	joinService     *JoinService
	pd              *PacketDelivery
}

func NewNodeEngine(
	mm *MembershipManager, db *KVStore,
	js *JoinService, c *Config,
	pd *PacketDelivery,
) *NodeEngine {
	n := &NodeEngine{
		config:          c,
		db:              db,
		mm:              mm,
		clusterPacketCh: make(chan *tcp.Packet),
		clientPacketCh:  make(chan *tcp.Packet),
		joinService:     js,
		pd:              pd,
	}
	return n
}

func (n *NodeEngine) Start() {
	fmt.Println("NodeEngine is starting!")

	// TODO fix this
	// n.mm.onPartitionTableUpdate = n.sendPartitionTableToReplicasAndClients
	n.clusterListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.config.IP, n.config.ClusterPort),
		PacketCh: n.clusterPacketCh,
	}
	if err := n.clusterListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clusterPacketCh, 1)

	memberAddresses := n.discoverMembers()
	joinedToLeader := n.joinService.Join(memberAddresses)
	if joinedToLeader {
		fmt.Println("Joined to an existing cluster!")
	} else {
		n.mm.DetermineLeader()
	}
	n.joinService.joinLock.Unlock()

	n.clientListener = &tcp.Listener{
		Addr:     fmt.Sprintf("%s:%s", n.config.IP, n.config.ClientPort),
		PacketCh: n.clientPacketCh,
	}
	if err := n.clientListener.Start(); err != nil {
		panic(err)
	}
	n.startPacketHandlerWorkers(n.clientPacketCh, 5)

	fmt.Println("NodeEngine is started!")
}

func (n *NodeEngine) discoverMembers() []string {
	var ds DiscoveryService
	if len(n.config.HeadlessDNS) > 0 {
		ds = &DNSDiscovery{Host: n.config.HeadlessDNS}
	} else {
		ds = &TcpIpDiscovery{MemberList: n.config.MemberList}
	}
	return ds.Discover()
}

func (n *NodeEngine) sendPartitionTableToReplicasAndClients() {
	if !n.mm.AmILeader() {
		return
	}

	go func() {
		if n.clientListener == nil {
			return
		}
		for _, clientConn := range n.clientListener.Connections {
			clientConn.Send(&membership.TopologyUpdatedEvent{Membership: n.mm.Membership})
		}
		fmt.Println("Shared latest mm details with clients!")
	}()
}

func (n *NodeEngine) startPacketHandlerWorkers(ch chan *tcp.Packet, workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for packet := range ch {
				n.pd.Deliver(packet)
			}
		}()
	}
}

func (n *NodeEngine) Close() {
	if n.clientListener != nil {
		n.clientListener.Close()
	}
	n.clusterListener.Close()
}
