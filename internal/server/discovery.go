package server

import (
	"fmt"
	"net"
	"time"
)

type DiscoveryService interface {
	Discover() []string
}

type DNSDiscovery struct {
	Host string
}

func (d *DNSDiscovery) Discover() []string {
	time.Sleep(2 * time.Second)
	ips, err := net.LookupIP(d.Host)
	if err != nil {
		fmt.Printf("[WARN] Couldn't resolve dns addr: %s err is: %v\n", d.Host, err)
		return []string{}
	}
	fmt.Println("Discovered ips:", ips)
	var addresses []string
	for _, ip := range ips {
		addresses = append(addresses, fmt.Sprintf("%s:6060", ip.String()))
	}
	return addresses
}

type TcpIpDiscovery struct {
	MemberList []string
}

func (t *TcpIpDiscovery) Discover() []string {
	return t.MemberList
}
