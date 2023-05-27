package main

import (
	"bytes"
	"dkvs/client"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	address := fmt.Sprintf("192.168.1.49:6050")
	c := client.NewClient(address)
	key := "name"
	c.Put(key, []byte("Serkan Erip"))

	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-t.C:
			val := c.Get(key)
			fmt.Printf("%s=%s\n", key, string(val))
		}
	}
}

func r1(c *client.Client) {
	v := []byte("TEST_DATA")
	var keys []string
	data := map[string][]byte{}
	for i := 0; i < 10_000; i++ {
		k := RandStringRunes(10)
		c.Put(k, v)
		data[k] = v
		keys = append(keys, k)
	}
	fmt.Println("Data prepared!")

	var ops uint64
	ch := make(chan func())
	for i := 0; i < 20; i++ {
		go func() {
			for f := range ch {
				f()
				atomic.AddUint64(&ops, 1)
			}
		}()
	}

	var wg sync.WaitGroup
	t := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(t) {
			fmt.Println("time is out!")
			close(ch)
			break
		}
		wg.Add(1)
		ch <- func() {
			k := keys[rand.Intn(len(keys))]
			if !bytes.Equal(c.Get(k), v) {
				fmt.Println("invalid data")
			}
			//fmt.Printf("[x]:%s\n", val)
			wg.Done()
		}
	}
	wg.Wait()
	fmt.Println("done, rps:", ops/30.0)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
