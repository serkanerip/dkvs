package server

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"log"
	"net"
	"os"
)

type Config struct {
	ID             uuid.UUID
	IP             string
	ClientPort     string   `mapstructure:"client-port"`
	ClusterPort    string   `mapstructure:"cluster-port"`
	MemberList     []string `mapstructure:"member-list"`
	PartitionCount int      `mapstructure:"partition-count"`
	HeadlessDNS    string   `mapstructure:"headless-dns"`
}

func NewConfig() *Config {
	configFilePath := envOrDefault("CONFIG_FILE", "config.yaml")
	viper.SetConfigFile(configFilePath)
	viper.SetDefault("client-port", "6050")
	viper.SetDefault("cluster-port", "6060")
	viper.SetDefault("partition-count", 23)
	viper.SetDefault("member-list", []string{})
	viper.AutomaticEnv()
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Printf("couldnt read config, err is:%v\n", err)
	}

	conf := &Config{}
	err = viper.Unmarshal(conf)
	if err != nil {
		fmt.Printf("unable to decode into config struct, %v\n", err)
	}
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

func envOrDefault(envName, def string) string {
	val, ok := os.LookupEnv(envName)
	if !ok {
		val = def
	}
	return val
}
