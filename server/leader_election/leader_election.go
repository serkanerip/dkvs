package leader_election

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaderElection struct {
	cli *clientv3.Client
	e   *concurrency.Election
	ctx context.Context
	s   *concurrency.Session
}

func NewLeaderElection(ctx context.Context, address string) *LeaderElection {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{address}})
	if err != nil {
		panic("couldnt connect to etcd")
	}
	fmt.Println("connected to etcd!")
	return &LeaderElection{cli: cli, ctx: ctx}
}

func (l *LeaderElection) Elect(id string) string {
	var err error
	// fmt.Println("getting new etcd session")
	l.s, err = concurrency.NewSession(l.cli, concurrency.WithTTL(10))
	if err != nil {
		panic(err)
	}
	// fmt.Println("getting new election")
	l.e = concurrency.NewElection(l.s, "/app-leader-election/")
	go func() {
		if err := l.e.Campaign(l.ctx, id); err != nil {
			if err != context.Canceled {
				panic(err)
			}
			return
		}
		fmt.Println("Claimed leadership")
	}()

	for {
		// Check if this instance became the leader
		leader, err := l.e.Leader(l.ctx)
		if err != nil {
			fmt.Println(err)
			continue
		}
		leaderID := string(leader.Kvs[0].Value)
		return leaderID
	}
}
