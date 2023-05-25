package common

import (
	"crypto/md5"
	"math/big"
)

func GetPartitionIDByKey(partitionCount int, key []byte) int {
	hash := md5.Sum(key)
	bigIntHash := new(big.Int).SetBytes(hash[:])
	partition := bigIntHash.Mod(bigIntHash, big.NewInt(int64(partitionCount)))
	return int(partition.Int64())
}
