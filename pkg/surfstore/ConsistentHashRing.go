package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap  map[string]string
	SortedKeys []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// find the first key that is greater than or equal to the hash
	for _, key := range c.SortedKeys {
		if key >= blockId {
			return c.ServerMap[key]
		}
	}
	// if no key is greater than or equal to the hash, return the first key
	return c.ServerMap[c.SortedKeys[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	log.Printf("Hashing %s\n", addr)
	return hex.EncodeToString(h.Sum(nil))
}

func (c ConsistentHashRing) AddServer(addr string) {
	hashString := c.Hash(addr)
	c.ServerMap[hashString] = addr
	c.SortedKeys = append(c.SortedKeys, hashString)
	sort.Strings(c.SortedKeys)
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := &ConsistentHashRing{ServerMap: make(map[string]string), SortedKeys: make([]string, 0)}

	for _, addr := range serverAddrs {
		hashString := hashRing.Hash(addr)
		hashRing.ServerMap[hashString] = addr
		hashRing.SortedKeys = append(hashRing.SortedKeys, hashString)
	}

	// sort the keys
	sort.Strings(hashRing.SortedKeys)

	return hashRing
}
