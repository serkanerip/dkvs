package server

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"log"
	"net"
)

type Config struct {
	ID             uuid.UUID
	IP             string
	ClientPort     string   `mapstructure:"CLIENT_PORT"`
	ClusterPort    string   `mapstructure:"CLUSTER_PORT"`
	MemberList     []string `mapstructure:"MEMBER_LIST"`
	PartitionCount int      `mapstructure:"PARTITION_COUNT"`
	HeadlessDNS    string   `mapstructure:"HEADLESS_DNS"`
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
