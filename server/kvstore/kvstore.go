package kvstore

import (
	"dkvs/common"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore struct {
	ownedPartitions   map[int]*partition
	partitionCount    int
	putOperationCount uint64
	getOperationCount uint64
	totalBytes        uint64
}

type partition struct {
	ID int
	ds sync.Map
}

func NewKVStore(partitionCount int, partitionIds ...int) *KVStore {
	kvStore := &KVStore{
		ownedPartitions: make(map[int]*partition),
		partitionCount:  partitionCount,
	}
	for _, id := range partitionIds {
		kvStore.ownedPartitions[id] = &partition{
			ID: id,
			ds: sync.Map{},
		}
	}
	// go kvStore.printStats()
	return kvStore
}

func (k *KVStore) printStats() {
	t := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-t.C:
			fmt.Println("STATS!")
			fmt.Println("Put Operations Count", k.putOperationCount)
			fmt.Println("Get Operations Count", k.getOperationCount)
			fmt.Println("Total Bytes", k.totalBytes)
		}
	}
}

func (k *KVStore) Put(key string, value []byte) {
	pid := k.partitionIdByKey([]byte(key))
	fmt.Println("key", key, "pid", pid, "value", value)
	k.ownedPartitions[pid].ds.Store(key, value)
	go func() {
		atomic.AddUint64(&k.putOperationCount, 1)
		atomic.AddUint64(&k.totalBytes, uint64(len(value)))
	}()
}

func (k *KVStore) Get(key string) []byte {
	pid := k.partitionIdByKey([]byte(key))
	fmt.Println("key", key, "pid", pid)
	val, _ := k.ownedPartitions[pid].ds.Load(key)
	if val == nil {
		fmt.Println("value is nil!")
		return nil
	}
	go func() {
		atomic.AddUint64(&k.getOperationCount, 1)
	}()
	return val.([]byte)
}

func (k *KVStore) partitionIdByKey(key []byte) int {
	return common.GetPartitionIDByKey(k.partitionCount, key)
}
