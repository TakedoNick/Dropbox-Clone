package surfstore

import (
	context "context"
	"log"
	"math"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	// Leaders
	nextIndex     map[int64]int64
	matchIndex    map[int64]int64
	isLeader      bool
	isLeaderMutex *sync.RWMutex

	// Followers
	commitIndex int64

	// All Servers
	thisServerAddr  string
	raftServerAddrs []string
	thisServerId    int64

	pendingCommits []*chan bool

	term int64
	log  []*UpdateOperation

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	// Read from metastore

	// Check for leader
	if err := s.checkLeaderOnly(ctx); err != nil {
		return nil, err
	}

	// Check leader crash
	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	// Check if majority of servers are up to query read
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// If majority up, then read and return
	if success.Flag {
		return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	}

	// If not, keep sending heartbeats until majority are up
	for !success.Flag {
		success, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {

	// Read from metastore

	// Check for leader
	if err := s.checkLeaderOnly(ctx); err != nil {
		return nil, err
	}

	// Check leader crash
	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	// Check if majority of servers are up to query read
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// If majority up, then read and return
	if success.Flag {
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	}

	// If not, keep sending heartbeats until majority are up
	for !success.Flag {
		success, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {

	// Read from metastore

	// Check for leader
	if err := s.checkLeaderOnly(ctx); err != nil {
		return nil, err
	}

	// Check leader crash
	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	// Check if majority of servers are up to query read
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// If majority up, then read and return
	if success.Flag {
		return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	}

	// If not, keep sending heartbeats until majority are up
	for !success.Flag {
		success, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	// return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	// Write to metastore

	// Check for leader
	if err := s.checkLeaderOnly(ctx); err != nil {
		return &Version{Version: int32(-1)}, err
	}

	// Check leader crash
	if err := s.checkCrash(ctx); err != nil {
		return &Version{Version: int32(-1)}, err
	}

	// Append op-entry to our log
	s.isLeaderMutex.Lock()
	updateOp := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta}

	s.log = append(s.log, &updateOp)

	// send entry to all followers in parallel
	// s.isFollowerMutex.Unlock()

	commitChan := make(chan bool)
	// curLogIndex := int64(len(s.log) - 1)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	s.isLeaderMutex.Unlock()

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// Channel innately blocks until majority back up
	commit := <-commitChan

	// // once committed, apply to the state machine

	if commit {
		// Check leader
		if err := s.checkLeaderOnly(ctx); err != nil {
			return nil, err
		}

		// Check leader crash
		if err := s.checkCrash(ctx); err != nil {
			return nil, err
		}
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil

}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {

	// TODO: May need to check for server crashing here
	// // if node not leader or leader crashed <- return false

	for {

		currCommitIndex := s.commitIndex
		lastLogIndex := int64(len(s.log) - 1)
		ips := s.raftServerAddrs
		serverID := s.thisServerId

		if currCommitIndex == lastLogIndex {
			// No more Appends to send
			break
		}

		nextCommitIndex := currCommitIndex + 1
		lastReplicatedIndex := int64(s.matchIndex[serverID])

		responses := make(chan *AppendEntryOutput, len(ips)-1)

		// contact all the followers, send some AppendEntries call
		for idx, addr := range ips {
			if int64(idx) == serverID {
				continue
			}

			// Parallely send Appends to followers if there are entries to be committed
			if nextCommitIndex > lastReplicatedIndex {
				go s.sendToFollower(ctx, int64(idx), addr, nextCommitIndex, responses)
			}
		}

		if err := s.checkCrash(ctx); err != nil {
			close(responses)
		}

		totalCommits := 1

		// blocking when majority not up
		for {
			response, ok := <-responses

			if response.Success {
				totalCommits++
			}

			// if channel close
			if !ok {
				return
			}

			if totalCommits > len(ips)/2 {
				*s.pendingCommits[nextCommitIndex] <- true
				s.commitIndex = nextCommitIndex
				break
			}
		}

	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, serverId int64, addr string, indexToCommit int64, responses chan *AppendEntryOutput) {

	// Set the index to commit as the followers nextIndex
	s.nextIndex[serverId] = indexToCommit

	for {

		currentInput := AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		currentInput.PrevLogIndex = s.nextIndex[serverId] - 1

		if currentInput.PrevLogIndex >= 0 {
			currentInput.PrevLogTerm = s.log[currentInput.PrevLogIndex].Term
		}

		currentInput.Entries = s.log[s.nextIndex[serverId]:]

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			conn.Close()
			log.Fatal("Dial error during sendToFollower")
		}

		r := NewRaftSurfstoreClient(conn)

		val, err := r.AppendEntries(ctx, &currentInput)
		// check if follower crashed <- since grpc.Dial wont throw a crash error as server is not actually crashed
		if err != nil {
			conn.Close()
			continue
		}

		// check if leader crashed
		if err2 := s.checkCrash(ctx); err2 != nil {
			conn.Close()
			return
		}

		currentTerm := s.term

		if !val.Success {
			if val.Term > currentTerm {
				// Term out of date -> update term
				s.isLeaderMutex.Lock()
				s.term = val.Term
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				conn.Close()
				continue
			} else {
				s.isLeaderMutex.Lock()
				s.nextIndex[serverId] = s.nextIndex[serverId] - 1
				s.isLeaderMutex.Unlock()
				conn.Close()
				continue
			}

		} else {
			// if success from Append Entry -> update nextIndex and matchIndex for the follower
			responses <- val
			s.nextIndex[serverId] = val.MatchedIndex + 1
			s.matchIndex[serverId] = val.MatchedIndex
			conn.Close()
			return
		}
	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	// if err := s.checkLeaderOnly(ctx); err != nil {
	// 	return nil, err
	// }

	// Check if the follower crashed
	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	currentTerm := s.term

	// default output
	output := &AppendEntryOutput{
		ServerId:     s.thisServerId,
		Term:         -1,
		Success:      false,
		MatchedIndex: -1,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term > currentTerm {
		// if Leader has a lower term then change it to a follower
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()

	} else if input.Term < currentTerm {
		// Reply false if term < currentTerm
		output.Term = currentTerm
		output.MatchedIndex = -1
		output.Success = false
		return output, nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// s.log has no entry at PrevLogIndex -> PrevLogIndex > the length of the log array
	// `>=` because len(array) > last_index(array)
	if input.PrevLogIndex >= 0 {
		if input.PrevLogIndex >= int64(len(s.log)) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			output.Term = s.term
			output.MatchedIndex = -1
			output.Success = false
			return output, nil
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)

	// if input.PrevLogIndex == -1 {
	// 	// Initial entry
	// 	s.log = make([]*UpdateOperation, 0)
	// } else {
	// 	existingEntryTerm := s.log[input.PrevLogIndex].Term
	// 	if existingEntryTerm != input.PrevLogTerm {
	// 		// slice the rest of the log from that entry's index
	// 		s.log = s.log[:input.PrevLogIndex]
	// 		output.Term = currentTerm
	// 		output.Success = false
	// 		output.MatchedIndex = 0
	// 		return output, nil
	// 	}
	// 	s.log = s.log[:input.PrevLogIndex+1]
	// }

	if int64(len(s.log)-1) > input.PrevLogIndex {
		s.log = s.log[:input.PrevLogIndex+1]
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// Update term of the follower
	s.term = input.Term

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	leaderCommit := input.LeaderCommit
	commitIndex := s.commitIndex
	indexOfLastEntry := int64(len(s.log) - 1)
	if leaderCommit > commitIndex {
		newIndex := int64(math.Min(float64(leaderCommit), float64(indexOfLastEntry)))

		for i := s.commitIndex + 1; i <= newIndex; i++ {
			entry := s.log[i]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
		s.commitIndex = newIndex
	}

	// for s.lastApplied < input.LeaderCommit {

	// 	entry := s.log[s.lastApplied+1]
	// 	s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	// 	s.lastApplied++
	// }

	// Passed all the above cases then update output
	output.Success = true
	output.MatchedIndex = indexOfLastEntry
	output.Term = s.term

	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	// Check for follower Crash
	if err := s.checkCrash(ctx); err != nil {
		return &Success{Flag: false}, err
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()

	// Set leader deterministically since there is no election
	s.isLeader = true
	s.term++

	// Node state -> won an election -> Update State
	for commit := len(s.pendingCommits); commit < int(s.commitIndex)+1; commit++ {
		comitted := make(chan bool)
		s.pendingCommits = append(s.pendingCommits, &comitted)
	}

	// Reinitialized after election
	// for each server, index of the next log entry to send to that server <- leaderLastLogIndex + 1
	// for each server, index of highest log entry known to be replicated on server = -1
	serverIPs := s.raftServerAddrs
	leaderLastLogIndex := int64(len(s.log)) - 1
	s.nextIndex = make(map[int64]int64)
	s.matchIndex = make(map[int64]int64)
	for server_no := range serverIPs {
		s.nextIndex[int64(server_no)] = leaderLastLogIndex + 1
		s.matchIndex[int64(server_no)] = -1
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	// if node not leader or leader crashed <- return false
	if err := s.checkLeaderOnly(ctx); err != nil {
		return &Success{Flag: false}, err
	}

	if err := s.checkCrash(ctx); err != nil {
		return &Success{Flag: false}, err
	}

	s.isLeaderMutex.RLock()
	ips := s.raftServerAddrs
	id := s.thisServerId
	s.isLeaderMutex.RUnlock()

	// concurrently send heartbeats to all servers except self(leader)
	heartbeatFromFollower := make(chan bool, len(ips)-1)
	for serverId := range ips {
		if serverId == int(id) {
			continue
		}

		s.isLeaderMutex.RLock()
		leaderTerm := s.term
		leaderCommit := s.commitIndex

		PrevLogIndex := s.matchIndex[int64(serverId)]
		PrevLogTerm := int64(-1)
		if PrevLogIndex >= 0 {
			PrevLogTerm = s.log[PrevLogIndex].Term
		}
		// lastLogIndex := len(s.log) - 1
		// if lastLogIndex >= int(nextIndex) {
		Entries := s.log[s.nextIndex[int64(serverId)]:]

		dummyEntry := &AppendEntryInput{
			Term: leaderTerm,
			// TODO put the right values
			PrevLogTerm:  PrevLogTerm,
			PrevLogIndex: PrevLogIndex,
			Entries:      Entries,
			LeaderCommit: leaderCommit,
		}

		s.isLeaderMutex.RUnlock()

		go s.sendHeartbeatInParallel(ctx, int64(serverId), dummyEntry, heartbeatFromFollower)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-heartbeatFromFollower
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(ips) {
			break
		}
	}

	if totalAppends > len(ips)/2 {
		return &Success{Flag: true}, nil
	}

	return &Success{Flag: false}, nil
}

func (s *RaftSurfstore) sendHeartbeatInParallel(ctx context.Context, serverId int64, dummy *AppendEntryInput, response chan<- bool) {

	// TODO: May need to keep trying to sendHeartBeats indefinitely
	for {
		conn, err := grpc.Dial(s.raftServerAddrs[serverId], grpc.WithTransportCredentials(insecure.NewCredentials()))
		// Dial error
		if err != nil {
			response <- false
			conn.Close()
			log.Fatal("Dial error during sendToFollower")
		}

		r := NewRaftSurfstoreClient(conn)

		// for {

		val, err := r.AppendEntries(ctx, dummy)

		// when a follower is crashed
		if err != nil {
			response <- false
			conn.Close()
			continue
		}

		// check if leader crashed
		if err2 := s.checkCrash(ctx); err2 != nil {
			response <- false
			conn.Close()
			return
		}

		currentTerm := s.term

		if val.Success {
			// if success from Append Entry -> update nextIndex and matchIndex for the follower
			response <- true
			s.nextIndex[serverId] = val.MatchedIndex + 1
			s.matchIndex[serverId] = val.MatchedIndex
			conn.Close()
			return
		} else {
			if val.Term > currentTerm {
				response <- false
				s.isLeaderMutex.Lock()
				s.term = val.Term
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				conn.Close()
				continue
			} else {
				s.nextIndex[serverId] = s.nextIndex[serverId] - 1
				response <- false
				conn.Close()
				continue
			}
		}
	}

}

// func (s *RaftSurfstore) checkLeader(ctx context.Context) error {

// 	// Leader or not
// 	s.isLeaderMutex.RLock()
// 	leader := s.isLeader
// 	s.isLeaderMutex.RUnlock()

// 	// Server Status
// 	s.isCrashedMutex.RLock()
// 	serverCrash := s.isCrashed
// 	s.isCrashedMutex.RUnlock()

// 	if !leader {
// 		return ERR_NOT_LEADER
// 	} else {
// 		if serverCrash {
// 			s.isLeaderMutex.Lock()
// 			s.isLeader = false
// 			s.isLeaderMutex.Unlock()
// 			return ERR_SERVER_CRASHED
// 		}
// 	}

// 	return nil
// }

func (s *RaftSurfstore) checkCrash(ctx context.Context) error {
	s.isCrashedMutex.RLock()
	serverCrash := s.isCrashed
	s.isCrashedMutex.RUnlock()
	// // Leader or not
	// s.isLeaderMutex.RLock()
	// leader := s.isLeader
	// s.isLeaderMutex.RUnlock()

	if serverCrash {
		return ERR_SERVER_CRASHED
	}
	// else if serverCrash && leader {
	// 	s.isLeaderMutex.Lock()
	// 	s.isLeader = false
	// 	s.isLeaderMutex.Unlock()
	// 	return ERR_SERVER_CRASHED
	// }

	return nil
}

func (s *RaftSurfstore) checkLeaderOnly(ctx context.Context) error {
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if leader {
		return nil
	}

	return ERR_NOT_LEADER
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
