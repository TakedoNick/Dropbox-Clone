package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	// Obtain all server hash keys and sort them
	hashes := []string{}
	for serverHash := range c.ServerMap {
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)

	// Find the first server with a larger hashvalue than the block
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}

	// Edge case where the blockhash is larger than all server hashes
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}

	return responsibleServer
	// panic("todo")
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	hexaddr := h.Sum(nil)
	return hex.EncodeToString(hexaddr)
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {

	servermap := make(map[string]string)
	for _, blockServerAddr := range serverAddrs {
		blockServerNameFormat := "blockstore" + blockServerAddr
		hashAddr := sha256.Sum256([]byte(blockServerNameFormat))
		servermap[hex.EncodeToString(hashAddr[:])] = blockServerAddr
	}
	return &ConsistentHashRing{ServerMap: servermap}
	// panic("todo")
}
