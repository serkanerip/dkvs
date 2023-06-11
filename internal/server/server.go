package server

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"log"
	"net"
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

type Config struct {
	ID             uuid.UUID
	IP             string
	ClientPort     string   `mapstructure:"CLIENT_PORT"`
	ClusterPort    string   `mapstructure:"CLUSTER_PORT"`
	MemberList     []string `mapstructure:"MEMBER_LIST"`
	PartitionCount int      `mapstructure:"PARTITION_COUNT"`
	HeadlessDNS    string   `mapstructure:"HEADLESS_DNS"`
}

const (
	IAmNotTheLeader           = "I_AM_NOT_THE_LEADER"
	ParticipationIsNotAllowed = "ParticipationIsNotAllowed"
)

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
	s.ne = NewNodeEngine(s.mm, s.js, c, s.pd)
	s.assignPacketReceivers()

	return s
}

func (s *Server) assignPacketReceivers() {
	s.pd.packetReceivers[message.JoinOP] = s.js
	s.pd.packetReceivers[message.JoinOPResp] = s.js
	s.pd.packetReceivers[message.MembershipUpdatedE] = s.mm
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

func NewConfig() *Config {
	viper.SetConfigFile(".env")
	viper.SetDefault("CLIENT_PORT", "6050")
	viper.SetDefault("CLUSTER_PORT", "6060")
	viper.SetDefault("PARTITION_COUNT", 23)
	viper.SetDefault("MEMBER_LIST", []string{})
	viper.AutomaticEnv()
	viper.BindEnv("HEADLESS_DNS")
	viper.BindEnv("CLIENT_PORT")
	viper.BindEnv("CLUSTER_PORT")
	viper.BindEnv("MEMBER_LIST")
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Printf("couldnt read config, err is:%v\n", err)
	}

	conf := &Config{}
	err = viper.Unmarshal(conf)
	if err != nil {
		fmt.Printf("unable to decode into config struct, %v\n", err)
	}
	fmt.Println("headlessdns", conf.HeadlessDNS)
	conf.ID = uuid.New()
	conf.IP = getOutboundIP().String()
	fmt.Printf("Starting with config below:\n%v\n", conf)
	return conf
}

func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
