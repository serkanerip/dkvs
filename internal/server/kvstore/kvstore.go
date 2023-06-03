package kvstore

import (
	"dkvs/pkg"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore struct {
	dataPartitions    map[int]*sync.Map
	partitionCount    int
	putOperationCount uint64
	getOperationCount uint64
	totalBytes        uint64
}

func NewKVStore(partitionCount int) *KVStore {
	kvStore := &KVStore{
		dataPartitions: make(map[int]*sync.Map),
		partitionCount: partitionCount,
	}
	for i := 0; i < partitionCount; i++ {
		kvStore.dataPartitions[i] = &sync.Map{}
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
	k.dataPartitions[pid].Store(key, value)
	go func() {
		atomic.AddUint64(&k.putOperationCount, 1)
		atomic.AddUint64(&k.totalBytes, uint64(len(value)))
	}()
}

func (k *KVStore) Get(key string) []byte {
	pid := k.partitionIdByKey([]byte(key))
	val, _ := k.dataPartitions[pid].Load(key)
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
	return pkg.GetPartitionIDByKey(k.partitionCount, key)
}
