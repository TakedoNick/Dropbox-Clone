package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	hostAddr := config.RaftAddrs[id]

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		nextIndex:       make(map[int64]int64),
		matchIndex:      make(map[int64]int64),
		commitIndex:     -1,
		pendingCommits:  make([]*chan bool, 0),
		thisServerAddr:  hostAddr,
		raftServerAddrs: config.RaftAddrs,
		thisServerId:    id,
		isLeader:        false,
		isLeaderMutex:   &isLeaderMutex,
		term:            0,
		metaStore:       NewMetaStore(config.BlockAddrs),
		log:             make([]*UpdateOperation, 0),
		isCrashed:       false,
		isCrashedMutex:  &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {

	grpc_raftserver := grpc.NewServer()

	RegisterRaftSurfstoreServer(grpc_raftserver, server)

	listener, err := net.Listen("tcp", server.thisServerAddr)
	if err != nil {
		return err
	}

	return grpc_raftserver.Serve(listener)
	// panic("todo")
}
