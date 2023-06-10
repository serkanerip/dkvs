package server

import (
	"dkvs/pkg"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore struct {
	dataPartitions    map[int]*DataPartition
	partitionCount    int
	putOperationCount uint64
	getOperationCount uint64
	totalBytes        uint64
}

type DataPartition struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewKVStore(partitionCount int) *KVStore {
	kvStore := &KVStore{
		dataPartitions: make(map[int]*DataPartition),
		partitionCount: partitionCount,
	}
	for i := 0; i < partitionCount; i++ {
		kvStore.dataPartitions[i] = &DataPartition{m: map[string][]byte{}}
	}
	// go kvStore.printStats()
	return kvStore
}

func (k *KVStore) printStats() {
	t := time.NewTicker(30 * time.Second)
	for range t.C {
		fmt.Println("STATS!")
		fmt.Println("Put Operations Count", k.putOperationCount)
		fmt.Println("Get Operations Count", k.getOperationCount)
		fmt.Println("Total Bytes", k.totalBytes)
	}
}

func (k *KVStore) Put(key string, value []byte) {
	pid := k.partitionIdByKey([]byte(key))
	dp := k.dataPartitions[pid]
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.m[key] = value
	go func() {
		atomic.AddUint64(&k.putOperationCount, 1)
		atomic.AddUint64(&k.totalBytes, uint64(len(value)))
	}()
}

func (k *KVStore) Get(key string) []byte {
	pid := k.partitionIdByKey([]byte(key))
	dp := k.dataPartitions[pid]
	dp.mu.Lock()
	defer dp.mu.Unlock()
	val := dp.m[key]
	if val == nil {
		fmt.Println("value is nil!")
		return nil
	}
	go func() {
		atomic.AddUint64(&k.getOperationCount, 1)
	}()
	return val
}

func (k *KVStore) partitionIdByKey(key []byte) int {
	return pkg.GetPartitionIDByKey(k.partitionCount, key)
}
