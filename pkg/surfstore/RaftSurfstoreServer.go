package surfstore

import (
	context "context"
	"fmt"
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
	commitIndex     int64
	lastApplied     int64
	isFollowerMutex *sync.RWMutex

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

	// Check Leader and Crashed states
	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	// Append op-entry to our log
	s.isLeaderMutex.Lock()
	updateOp := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta}

	s.log = append(s.log, &updateOp)

	// send entry to all followers in parallel
	// s.isFollowerMutex.Unlock()

	commitChan := make(chan bool, 1)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	s.isLeaderMutex.Unlock()

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat
	// Check if majority of servers are up to query read

	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// // If majority up, then read and return
	// if success.Flag {
	// 	commit := <-commitChan

	// 	// once committed, apply to the state machine
	// 	if commit {
	// 		return s.metaStore.UpdateFile(ctx, filemeta)
	// 	}
	// }

	// If not, keep sending heartbeats until majority are up
	for !success.Flag {
		success, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	commit := <-commitChan

	// once committed, apply to the state machine

	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return &Version{Version: -1}, fmt.Errorf("server error: error updating file")
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {

	// TODO: May need to check for server crashing here
	// if node not leader or leader crashed <- return false
	if err := s.checkLeaderOnly(ctx); err != nil {
		s.isFollowerMutex.Lock()
		// curLog := s.log
		// *s.pendingCommits[int64(len(curLog)-1)] <- false
		*s.pendingCommits[s.lastApplied+1] <- false
		// TODO update commit Index correctly
		// s.commitIndex = int64(len(curLog) - 1)
		s.isFollowerMutex.Unlock()
		return
	}

	if err := s.checkCrash(ctx); err != nil {
		s.isFollowerMutex.Lock()
		// curLog := s.log
		// *s.pendingCommits[int64(len(curLog)-1)] <- false
		*s.pendingCommits[s.lastApplied+1] <- false
		// TODO update commit Index correctly
		// s.commitIndex = int64(len(curLog) - 1)
		s.isFollowerMutex.Unlock()
		return
	}

	s.isFollowerMutex.RLock()
	ips := s.raftServerAddrs
	serverID := s.thisServerId
	s.isFollowerMutex.RUnlock()

	responses := make(chan bool, len(ips)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range ips {
		if int64(idx) == serverID {
			continue
		}

		go s.sendToFollower(ctx, int64(idx), addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(ips) {
			break
		}
	}

	s.isFollowerMutex.Lock()
	// curLog := s.log
	if totalAppends > len(s.raftServerAddrs)/2 {
		// TODO put on correct channel
		// for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		// 	*s.pendingCommits[i] <- true
		// }
		*s.pendingCommits[s.lastApplied+1] <- true
		// *s.pendingCommits[int64(len(curLog)-1)] <- true
		// TODO update commit Index correctly
		s.commitIndex++
	} else {
		// for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		// 	*s.pendingCommits[i] <- false
		// }
		*s.pendingCommits[s.lastApplied+1] <- false
		// TODO update commit Index correctly
		// s.commitIndex = int64(len(curLog) - 1)
	}
	s.isFollowerMutex.Unlock()
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, serverId int64, addr string, responses chan bool) {

	// if node not leader or leader crashed <- return false
	// if err := s.checkLeader(ctx); err != nil {
	// 	if err == ERR_SERVER_CRASHED {
	// 		responses <- false
	// 		return
	// 	}
	// }

	s.isFollowerMutex.RLock()
	currentInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	nextIndex := s.nextIndex[serverId]
	currentInput.PrevLogIndex = nextIndex - 1

	if nextIndex == 0 {
		currentInput.PrevLogTerm = int64(0)
	} else {
		currentInput.PrevLogTerm = s.log[currentInput.PrevLogIndex].Term
	}

	currentInput.Entries = s.log[nextIndex:]

	s.isFollowerMutex.RUnlock()

	// TODO check all errors
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Println("Connection Error in sendToFollower: ", err)
	}

	r := NewRaftSurfstoreClient(conn)

	for {

		// if err := s.checkCrash(ctx); err != nil {
		// 	if err == ERR_SERVER_CRASHED {
		// 		responses <- false
		// 		conn.Close()
		// 		return
		// 	}
		// }

		val, err := r.AppendEntries(ctx, &currentInput)

		if err != nil {
			responses <- false
			conn.Close()
			return
		}

		s.isFollowerMutex.RLock()
		currentTerm := s.term
		s.isFollowerMutex.RUnlock()

		if !val.Success {
			if val.Term > currentTerm {
				// Term out of date -> update term
				s.isFollowerMutex.Lock()
				s.term = val.Term
				s.isLeader = false
				s.isFollowerMutex.Unlock()
				responses <- false
				conn.Close()
				return
			} else {
				s.isFollowerMutex.Lock()
				s.nextIndex[serverId] = s.nextIndex[serverId] - 1
				s.isFollowerMutex.Unlock()
				// responses <- false
				// conn.Close()
				// return
				continue
			}

		} else {
			// if success from Append Entry -> update nextIndex and matchIndex for the follower
			s.isFollowerMutex.Lock()
			s.nextIndex[serverId] = val.MatchedIndex + 1
			s.matchIndex[serverId] = val.MatchedIndex
			s.isFollowerMutex.Unlock()
			responses <- true
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

	if err := s.checkCrash(ctx); err != nil {
		return nil, err
	}

	s.isFollowerMutex.Lock()
	defer s.isFollowerMutex.Unlock()

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

		// if a followers term is lower than input.Term, we need to update its term
		s.term = input.Term
	} else if input.Term < currentTerm {
		// Reply false if term < currentTerm
		output.Term = currentTerm
		output.MatchedIndex = 0
		output.Success = false
		return output, nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// s.log has no entry at PrevLogIndex -> PrevLogIndex > the length of the log array
	// `>=` because len(array) > last_index(array)
	if input.PrevLogIndex >= int64(len(s.log)) {
		output.Term = s.term
		output.MatchedIndex = 0
		output.Success = false
		return output, nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)

	if input.PrevLogIndex == -1 {
		// Initial entry
		s.log = make([]*UpdateOperation, 0)
	} else {
		existingEntryTerm := s.log[input.PrevLogIndex].Term
		if existingEntryTerm != input.PrevLogTerm {
			// slice the rest of the log from that entry's index
			s.log = s.log[:input.PrevLogIndex]
			output.Term = currentTerm
			output.Success = false
			output.MatchedIndex = 0
			return output, nil
		}
		s.log = s.log[:input.PrevLogIndex+1]
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	leaderCommit := input.LeaderCommit
	commitIndex := s.commitIndex
	indexOfLastEntry := int64(len(s.log) - 1)
	if leaderCommit > commitIndex {
		s.commitIndex = int64(math.Min(float64(leaderCommit), float64(indexOfLastEntry)))
	}

	for s.lastApplied < input.LeaderCommit {

		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	// Passed all the above cases then
	output.Success = true
	output.MatchedIndex = indexOfLastEntry
	output.Term = currentTerm

	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	if err := s.checkCrash(ctx); err != nil {
		return &Success{Flag: false}, err
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()

	// Set leader deterministically since there is no election
	s.isLeader = true
	s.term++

	// Node state -> won an election -> Update State

	// Reinitialized after election
	// for each server, index of the next log entry to send to that server <- leaderLastLogIndex + 1
	// for each server, index of highest log entry known to be replicated on server = -1
	serverIPs := s.raftServerAddrs
	leaderLastLogIndex := int64(len(s.log)) - 1
	for server_no := range serverIPs {
		s.nextIndex[int64(server_no)] = leaderLastLogIndex + 1
		s.matchIndex[int64(server_no)] = leaderLastLogIndex
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
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.commitIndex,
	}
	ips := s.raftServerAddrs
	id := s.thisServerId
	s.isLeaderMutex.RUnlock()

	// concurrently send heartbeats to all servers except self(leader)
	heartbeatFromFollower := make(chan bool, len(ips)-1)
	for serverId := range ips {
		if serverId == int(id) {
			continue
		}

		go s.sendHeartbeatInParallel(ctx, int64(serverId), &dummyAppendEntriesInput, heartbeatFromFollower)
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
	// if err := s.checkCrash(ctx); err != nil {
	// 	response <- false
	// 	return
	// }

	s.isFollowerMutex.RLock()

	nextIndex := s.nextIndex[serverId]
	dummy.PrevLogIndex = nextIndex - 1

	if nextIndex == 0 {
		dummy.PrevLogTerm = int64(0)
	} else {
		dummy.PrevLogTerm = s.log[dummy.PrevLogIndex].Term
	}

	lastLogIndex := len(s.log) - 1
	if lastLogIndex >= int(nextIndex) {
		dummy.Entries = s.log[nextIndex:]
	}

	s.isFollowerMutex.RUnlock()

	conn, err := grpc.Dial(s.raftServerAddrs[serverId], grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		fmt.Println("Connection Error in Sending Heartbeat: ", err)
	}

	r := NewRaftSurfstoreClient(conn)

	for {
		// if err := s.checkCrash(ctx); err != nil {
		// 	if err == ERR_SERVER_CRASHED {
		// 		response <- false
		// 		conn.Close()
		// 		return
		// 	}
		// }

		val, err := r.AppendEntries(ctx, dummy)

		if err != nil {
			response <- false
			conn.Close()
			return
		}

		s.isFollowerMutex.RLock()
		currentTerm := s.term
		s.isFollowerMutex.RUnlock()

		if !val.Success {
			if val.Term > currentTerm {
				// Term out of date -> update term
				s.isFollowerMutex.Lock()
				s.term = val.Term
				s.isLeader = false
				s.isFollowerMutex.Unlock()
				response <- false
				conn.Close()
				return
			} else {
				s.isFollowerMutex.Lock()
				s.nextIndex[serverId] = s.nextIndex[serverId] - 1
				s.isFollowerMutex.Unlock()
				// response <- false
				// conn.Close()
				// return
				continue
			}

		} else {
			// if success from Append Entry -> update nextIndex and matchIndex for the follower
			s.isFollowerMutex.Lock()
			s.nextIndex[serverId] = val.MatchedIndex + 1
			s.matchIndex[serverId] = val.MatchedIndex
			s.isFollowerMutex.Unlock()
			response <- true
			conn.Close()
			return
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
	// Leader or not
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if serverCrash && !leader {
		return ERR_SERVER_CRASHED
	} else if serverCrash && leader {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		return ERR_SERVER_CRASHED
	}

	return nil
}

func (s *RaftSurfstore) checkLeaderOnly(ctx context.Context) error {
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !leader {
		return ERR_NOT_LEADER
	}

	return nil
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
