package raft

import (
	"encoding/gob"
	"errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pwzgorilla/miniraft/proto"
)

//Constansts indicating the replica's state
const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

const (
	minTimeout           = 2000 //in milliseconds
	maxTimeout           = 4000
	heartbeatMsgInterval = 800
	responseTimeout      = 1000
)

type ErrRedirect int // See Log.Append. Implements Error interface.
type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data []byte) (proto.LogEntry, error)
}

// Raft setup
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	ClusterConfig *ClusterConfig
	ServerID      int
	CommitCh      chan proto.LogEntry
	LeaderID      int
	CurrentState  string //state of the server

	//entries for implementing Shared Log
	LogObj               *proto.Log
	EventInCh            chan proto.Event
	ServerQuitCh         chan chan struct{}
	ElectionTimer        <-chan time.Time
	Term                 uint64
	LastVotedTerm        uint64
	LastVotedCandidateID int
	ReplicaChannels      map[int]*gob.Encoder //map of replica id to socket

	timer *time.Timer

	running *proto.AtomicBool

	//Fields required in case the server is leader
	nextIndex  map[int]proto.Lsn
	matchIndex map[int]proto.Lsn

	eventListener net.Listener
	Mutex         *sync.Mutex
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan proto.LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.ClusterConfig = config
	raft.ServerID = thisServerId
	raft.CommitCh = commitCh
	//	raft.AppendRequestCh = make(chan handler.AppendRequestMessage)
	//	raft.LogEntryBuffer = make([]LogEntry, 0)
	raft.LastVotedCandidateID = -1
	raft.LastVotedTerm = 0
	raft.ReplicaChannels = make(map[int]*gob.Encoder)
	raft.nextIndex = make(map[int]proto.Lsn)
	raft.matchIndex = make(map[int]proto.Lsn)

	raft.LogObj = proto.NewLog(raft.ServerID) //State the file name
	raft.LogObj.SendToStateMachine = func(entry *proto.LogEntryObj) {
		raft.CommitCh <- *entry
	}
	raft.LogObj.FirstRead()
	raft.Term = raft.LogObj.LastTerm()

	//raft.timer = time.NewTimer(getRandomWaitDuration())

	raft.running = new(proto.AtomicBool)
	raft.Mutex = &sync.Mutex{}
	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func (raft *Raft) Append(data []byte) (proto.LogEntry, error) {
	if len(data) > 0 {
		var logEntry proto.LogEntryObj
		logEntry = proto.LogEntryObj{raft.LogObj.LastIndex() + 1, data, false, raft.Term}
		raft.LogObj.AppendEntry(logEntry)
		return logEntry, nil
	} else {
		return nil, new(ErrRedirect)
	}
}

func (raft *Raft) Loop() {
	raft.CurrentState = FOLLOWER // begin life as a follower
	for raft.running.Get() {
		log.Println("Server", raft.ServerID, "in term", raft.Term, "in state ", raft.CurrentState)

		switch raft.CurrentState {
		case FOLLOWER:
			raft.CurrentState = raft.Follower()
		case CANDIDATE:
			raft.CurrentState = raft.Candidate()
		case LEADER:
			raft.CurrentState = raft.Leader()
		default:
			//raft.Mutex.Unlock()
			log.Println("Error: Unknown server state")
			return
		}
	}
}

func (raft *Raft) Follower() string {
	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())

	for raft.running.Get() {
		select {
		case <-raft.timer.C:
			log.Printf("At server %d, heartbeat timeout occured. State changing from %s to %s", raft.ServerID, raft.CurrentState, CANDIDATE)
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			//raft.timer.Reset(getRandomWaitDuration())
			return CANDIDATE // new state back to loop()

		case event := <-raft.EventInCh:
			if event.Data != nil {
				switch event.Type {
				case proto.TypeClientAppendRequest:
					// Do not handle clients in follower mode. Send it back up the
					// pipe with committed = false
					message := event.Data.(proto.ClientAppendRequest)
					cmd, _ := proto.DecodeCommand(message.Data)
					if cmd.Action == proto.StopServer {
						//Launch even for stopping the server
						raft.StopServer()
						break
					}

					leaderConfig := raft.ClusterConfig.Servers[0]
					for _, server := range raft.ClusterConfig.Servers {
						if server.Id == raft.LeaderID {
							leaderConfig = server
						}
					}

					var responseMsg string
					if leaderConfig.Id != raft.LeaderID {
						responseMsg = "ERR_REDIRECT NA\r\n"
					} else {
						responseMsg = "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

					}
					(*message.ResponseCh) <- responseMsg

				case proto.TypeVoteRequest:
					raft.timer.Reset(getRandomWaitDuration())

					message := event.Data.(proto.VoteRequest)

					voteResp, _ := raft.validateVoteRequest(message)
					go raft.sendToServerReplica(&proto.Event{proto.TypeVoteReply, voteResp}, message.CandidateID)
					raft.LeaderID = message.CandidateID
					raft.LastVotedCandidateID = message.CandidateID
					raft.LastVotedTerm = message.Term

				case proto.TypeHeartBeat:
					message := event.Data.(proto.AppendEntryRequest)
					log.Printf("At Server %d, received HeartBeat meassage from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					raft.timer.Reset(getRandomWaitDuration())

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending HeartBeat response to leader %d. Expected index is %d", raft.ServerID, message.LeaderID, resp.ExpectedIndex)

					go raft.sendToServerReplica(&proto.Event{proto.TypeHeartBeatResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case proto.TypeAppendEntryRequest:
					raft.timer.Reset(getRandomWaitDuration())

					message := event.Data.(proto.AppendEntryRequest)
					log.Printf("At Server %d, received AppendEntryResquest from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending AppendEntryResponse to leader %d", raft.ServerID, message.LeaderID)

					go raft.sendToServerReplica(&proto.Event{proto.TypeAppendEntryResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case proto.TypeTimeout:

				}
			}
		}
	}
	return raft.CurrentState
}

func (raft *Raft) Candidate() string {
	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())
	ackCount := 0

	//send out vote request to all the replicas
	voteRequestMsg := proto.VoteRequest{
		Term:         raft.Term,
		CandidateID:  raft.ServerID,
		LastLogIndex: raft.LogObj.LastIndex(),
		LastLogTerm:  raft.LogObj.LastTerm(),
	}

	go func() {
		log.Printf("Server %d send VoteMsg to others", raft.ServerID)
		raft.sendVoteRequest(&voteRequestMsg)
	}()

	raft.LastVotedTerm = raft.Term

	for raft.running.Get() {
		select {
		case <-raft.timer.C:
			log.Printf("At server %d, Election ended with no leader", raft.ServerID)
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			//raft.timer.Reset(getRandomWaitDuration())
			return CANDIDATE // new state back to loop()

		case event := <-raft.EventInCh:
			if event.Data != nil {
				switch event.Type {
				case proto.TypeClientAppendRequest:
					// Do not handle clients in follower mode. Send it back up the
					// pipe with committed = false
					message := event.Data.(proto.ClientAppendRequest)

					cmd, _ := proto.DecodeCommand(message.Data)
					if cmd.Action == proto.StopServer {
						//Launch even for stopping the server
						raft.StopServer()
						break
					}

					leaderConfig := raft.ClusterConfig.Servers[0]
					for _, server := range raft.ClusterConfig.Servers {
						if server.Id == raft.LeaderID {
							leaderConfig = server
						}
					}

					var responseMsg string
					if leaderConfig.Id != raft.LeaderID {
						responseMsg = "ERR_REDIRECT " + "NA"
					} else {
						responseMsg = "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

					}
					(*message.ResponseCh) <- responseMsg

				case proto.TypeVoteRequest:
					message := event.Data.(proto.VoteRequest)
					log.Println("Server ", raft.ServerID, " Received vote request from", message.CandidateID)
					resp, changeState := raft.validateVoteRequest(message)

					go func() {
						raft.sendVoteReply(&resp, message.CandidateID)
					}()

					if changeState {
						raft.LeaderID = -1 //TODO: changed from  -1 to message.CandidateID
						raft.LastVotedCandidateID = message.CandidateID
						raft.LastVotedTerm = message.Term
						log.Println("Server ", raft.ServerID, " changing state to ", FOLLOWER)
						return FOLLOWER
					}

				case proto.TypeVoteReply:
					message := event.Data.(proto.VoteReply)
					if message.Term > raft.Term {
						log.Printf("At server %d got vote from future term (%d>%d); abandoning election\n", raft.ServerID, message.Term, raft.Term)
						raft.LeaderID = -1
						raft.LastVotedCandidateID = -1
						return FOLLOWER
					}

					if message.Term < raft.Term {
						log.Printf("Server %d got vote from past term (%d<%d); ignoring\n", raft.ServerID, message.Term, raft.Term)
						break
					}

					if message.Result {
						log.Printf("At server %d, received vote from %d\n", raft.ServerID, message.ServerID)
						ackCount++
					}
					// "Once a candidate wins an election, it becomes leader."
					if (ackCount + 1) >= (len(raft.ClusterConfig.Servers)/2 + 1) {
						log.Println("At server ", raft.ServerID, " Selected leaderID = ", raft.ServerID)
						raft.LeaderID = raft.ServerID
						raft.LastVotedCandidateID = -1
						ackCount = 0
						return LEADER
					}

				case proto.TypeHeartBeat:
					message := event.Data.(proto.HeartBeatRequest)
					log.Printf("At Server %d, received HeartBeat meassage from %d", raft.ServerID, message.LeaderID)

					resp, changeOfLeader := raft.validateHeartBeatRequest(message)

					log.Printf("At Server %d, sending HeartBeat response to leader %d", raft.ServerID, message.LeaderID)
					go func() {
						raft.sendHeartBeatReply(&resp, message.LeaderID)
					}()

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case proto.TypeAppendEntryRequest:
					message := event.Data.(proto.AppendEntryRequest)
					log.Printf("At Server %d, received AppendEntryResquest from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending AppendEntryResponse to leader %d", raft.ServerID, message.LeaderID)

					go raft.sendToServerReplica(&proto.Event{proto.TypeAppendEntryResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case proto.TypeTimeout:

				}
			}
		}
	}
	return raft.CurrentState
}

func (raft *Raft) Leader() string {
	//Initialize the nextIndex and matchIndexStructures
	for _, server := range raft.ClusterConfig.Servers {
		if server.Id != raft.ServerID {
			raft.nextIndex[server.Id] = raft.LogObj.LastIndex() + 1
			raft.matchIndex[server.Id] = 0
		}
	}

	//Start hearbeat sending routine
	go raft.sendHeartbeat()

	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())
	ackCount := 0
	nakCount := 0
	var previousLogEntryForConsensus proto.LogEntry
	var messagePendingForConsensus proto.Event

	for raft.running.Get() {
		select {
		case event := <-raft.EventInCh:
			switch event.Type {
			case proto.TypeClientAppendRequest:

				log.Printf("At Server %d, received client append request", raft.ServerID)
				appendRPCMessage := proto.AppendEntryRequest{
					LeaderID:          raft.ServerID,
					LeaderCommitIndex: raft.LogObj.GetCommitIndex(),
					PreviousLogIndex:  raft.LogObj.LastIndex(),
					Term:              raft.Term,
					PreviousLogTerm:   raft.LogObj.LastTerm(),
				}

				message := event.Data.(proto.ClientAppendRequest)
				cmd, _ := proto.DecodeCommand(message.Data)
				if cmd.Action == proto.StopServer {
					//Launch even for stopping the server
					raft.StopServer()
					break
				}

				logEntry, err := raft.Append(message.Data)
				if err != nil {
					//TODO: this case would never happen
					*message.ResponseCh <- "ERR_APPEND"
					continue
				}

				//put entry in the global map
				proto.ResponseChannelStore.Lock()
				proto.ResponseChannelStore.M[logEntry.Lsn()] = message.ResponseCh
				proto.ResponseChannelStore.Unlock()

				previousLogEntryForConsensus = logEntry

				//now check for consensus
				appendRPCMessage.LogEntries = make([]proto.LogEntryObj, 0)
				appendRPCMessage.LogEntries = append(appendRPCMessage.LogEntries, logEntry.(proto.LogEntryObj))

				log.Printf("At Server %d, sending TypeAppendEntryRequest", raft.ServerID)

				messagePendingForConsensus = proto.Event{proto.TypeAppendEntryRequest, appendRPCMessage}
				go raft.sendToServerReplica(&messagePendingForConsensus, proto.Broadcast)

			case proto.TypeVoteRequest:

				message := event.Data.(proto.VoteRequest)
				resp, changeState := raft.validateVoteRequest(message)

				go raft.sendToServerReplica(&proto.Event{proto.TypeVoteReply, resp}, message.CandidateID)
				if changeState {
					log.Printf("At Server %d, change of leader from %d to %d\n", raft.ServerID, raft.LeaderID, message.CandidateID)
					raft.LeaderID = message.CandidateID //TODO: change from -1 to
					return FOLLOWER
				}

			case proto.TypeAppendEntryRequest:
				message := event.Data.(proto.AppendEntryRequest)
				log.Printf("At server %d, Error - Two servers in leader state found. Server %d at term %d, Server %d at term %d", raft.ServerID, raft.ServerID, raft.Term, message.LeaderID, message.Term)
				resp, changeState := raft.validateAppendEntryRequest(message)

				if message.LogEntries != nil {
					//send serponse to the leader
					go raft.sendToServerReplica(&proto.Event{proto.TypeAppendEntryResponse, resp}, message.LeaderID)
				}

				if changeState {
					raft.LeaderID = message.LeaderID
					log.Printf("At server %d, Changing state to FOLLOWER", raft.ServerID)
					return FOLLOWER
				}

			case proto.TypeHeartBeatResponse:
				message := event.Data.(proto.AppendEntryResponse)
				raft.nextIndex[message.ServerID] = message.ExpectedIndex

			case proto.TypeAppendEntryResponse:
				// Do not handle clients in follower mode. Send it back up the
				// pipe with committed = false
				message := event.Data.(proto.AppendEntryResponse)
				log.Printf("At Server %d, received AppendEntryResponse", raft.ServerID)

				if message.Term == raft.Term {
					if message.Success {
						ackCount++

						if (ackCount + 1) == (len(raft.ClusterConfig.Servers)/2 + 1) {
							//TODO: commit the log entry - write to disk

							//Now send the entry to KV store
							raft.CommitCh <- previousLogEntryForConsensus
							//							raft.Mutex.Unlock()

							log.Printf("At Server %d, committing to index %d", raft.ServerID, raft.LogObj.LastIndex()-1)
							raft.LogObj.CommitTo(raft.LogObj.LastIndex() - 1)

							//reset ackCount
							ackCount = 0
							nakCount = 0
						}
					} else {
						log.Printf("At Server %d, received negative response from server %d", raft.ServerID, message.ServerID)
						nakCount++

						if (nakCount + 1) == (len(raft.ClusterConfig.Servers)/2 + 1) {
							timer := time.NewTimer(time.Duration(responseTimeout) * time.Millisecond)

							/*
								proto.ResponseChannelStore.RLock()
								responseChannel := proto.ResponseChannelStore.M[previousLogEntryForConsensus.Lsn()]
								proto.ResponseChannelStore.RUnlock()

								if responseChannel == nil {
									log.Printf("At server %d, Response channel for LogEntry with lsn %d not found", raft.ServerID, int(previousLogEntryForConsensus.Lsn()))
								} else {
									//Delete the entry for response channel handle
									proto.ResponseChannelStore.Lock()
									delete(proto.ResponseChannelStore.M, previousLogEntryForConsensus.Lsn())
									proto.ResponseChannelStore.Unlock()
									*responseChannel <- "ERR_QUORUM_NOT_ACHIEVED\r\n"
								}
							*/

							//wait for some time before making a new request
							<-timer.C

							//reset ackCount and nakCount
							nakCount = 0
							ackCount = 0
							go raft.sendToServerReplica(&messagePendingForConsensus, proto.Broadcast)
						}

					}
				} else {
					log.Printf("At Server %d, received AppendEntryResponse for older term (%d>%d)", raft.ServerID, raft.Term, message.Term)
				}

			case proto.TypeTimeout:
			}
		case <-raft.timer.C:
		}
	}
	return raft.CurrentState

}

func (raft *Raft) InitServer() {
	//register for RPC
	gob.Register(proto.LogEntryObj{})
	gob.Register(proto.Event{})
	gob.Register(proto.VoteRequest{})
	gob.Register(proto.VoteReply{})
	gob.Register(proto.AppendEntryRequest{})
	gob.Register(proto.AppendEntryResponse{})
	gob.Register(proto.ClientAppendRequest{})
	gob.Register(proto.Timeout{})

	go raft.startListeningForReplicaEvents()

	go raft.createReplicaConnections()

	raft.running.Set(true)
	raft.Loop()
}

func (raft *Raft) validateVoteRequest(req proto.VoteRequest) (proto.VoteReply, bool) {
	if req.Term <= raft.Term {
		return proto.VoteReply{raft.Term, false, raft.ServerID}, false
	}

	changeState := false
	if req.Term > raft.Term {
		log.Printf("At Server %d, Vote Request with newer term (%d)", raft.ServerID, req.Term)
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		raft.LeaderID = -1
		changeState = true
	}

	if (raft.CurrentState == LEADER && !changeState) ||
		(raft.LastVotedCandidateID != -1 && raft.LastVotedCandidateID != req.CandidateID) ||
		(raft.LogObj.LastIndex() > req.LastLogIndex || raft.LogObj.LastTerm() > req.LastLogTerm) {
		log.Printf("At Server %d, sending negative vote reply to server %d", raft.ServerID, req.CandidateID)
		return proto.VoteReply{raft.Term, false, raft.ServerID}, changeState
	} else {
		raft.LastVotedCandidateID = req.CandidateID
		raft.timer.Reset(getRandomWaitDuration())
		log.Printf("At Server %d, sending positive vote reply to server %d", raft.ServerID, req.CandidateID)
		return proto.VoteReply{raft.Term, true, raft.ServerID}, changeState
	}

}

func (raft *Raft) validateHeartBeatRequest(req proto.HeartBeatRequest) (proto.HeartBeatReply, bool) {
	if req.Term < raft.Term {
		return proto.HeartBeatReply{
			Term:     raft.Term,
			LeaderID: raft.LeaderID,
			Success:  false,
		}, false
	}

	return proto.HeartBeatReply{
		Term:     req.Term,
		LeaderID: req.LeaderID,
		Success:  true,
	}, true
}

func (raft *Raft) validateAppendEntryRequest(req proto.AppendEntryRequest) (proto.AppendEntryResponse, bool) {
	expectedIndex := raft.LogObj.LastIndex() + 1
	if raft.LogObj.LastIndex() == 0 {
		expectedIndex = 1
	}

	if req.Term < raft.Term {
		return proto.AppendEntryResponse{
			Term:             raft.Term,
			Success:          false,
			ServerID:         raft.ServerID,
			PreviousLogIndex: raft.LogObj.LastIndex(),
			ExpectedIndex:    expectedIndex,
		}, false
	}
	stepDown := false

	if req.Term > raft.Term {
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		stepDown = true
		if raft.CurrentState == LEADER {
			log.Printf("At Server %d, AppendEntryRequest with higher term %d received from leader %d", raft.ServerID, raft.Term, req.LeaderID)
		}
		log.Printf("At Server %d, new leader is %d", raft.ServerID, req.LeaderID)
	}

	if raft.CurrentState == CANDIDATE && req.LeaderID != raft.LeaderID && req.Term >= raft.Term {
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		stepDown = true
		raft.LeaderID = req.LeaderID
	}

	raft.timer.Reset(getRandomWaitDuration())

	// Reject if log doesn't contain a matching previous entry
	log.Printf("At Server %d, Performing log discard check with index = %d", raft.ServerID, req.PreviousLogIndex)

	err := raft.LogObj.Discard(req.PreviousLogIndex, req.PreviousLogTerm)

	if err != nil {
		log.Printf("At Server %d, Log discard check failed - %s", raft.ServerID, err)
		return proto.AppendEntryResponse{
			Term:             raft.Term,
			Success:          false,
			ServerID:         raft.ServerID,
			PreviousLogIndex: raft.LogObj.LastIndex(),
			ExpectedIndex:    expectedIndex,
		}, stepDown
	}

	var entry proto.LogEntry
	if req.LogEntries != nil && len(req.LogEntries) == 1 {
		entry = req.LogEntries[0]
		if entry != nil {
			// Append entry to the log
			if err := raft.LogObj.AppendEntry(entry.(proto.LogEntryObj)); err != nil {
				log.Printf("At Server %d, Log Append failed - %s", raft.ServerID, err.Error())

				return proto.AppendEntryResponse{
					Term:             raft.Term,
					Success:          false,
					ServerID:         raft.ServerID,
					PreviousLogIndex: raft.LogObj.LastIndex(),
					ExpectedIndex:    expectedIndex,
				}, stepDown
			}
		}
	} else if req.LogEntries != nil && len(req.LogEntries) > 1 {
		if err := raft.LogObj.AppendEntries(req.LogEntries); err != nil {
			log.Printf("At Server %d, Log Append failed - %s", raft.ServerID, err.Error())

			return proto.AppendEntryResponse{
				Term:             raft.Term,
				Success:          false,
				ServerID:         raft.ServerID,
				PreviousLogIndex: raft.LogObj.LastIndex(),
				ExpectedIndex:    expectedIndex,
			}, stepDown
		}
	}

	if req.LeaderCommitIndex > 0 && req.LeaderCommitIndex > raft.LogObj.GetCommitIndex() {
		log.Printf("At Server %d, Committing to index %d", raft.ServerID, req.LeaderCommitIndex)
		lastCommitIndex, _ := raft.LogObj.CommitInfo()

		if err := raft.LogObj.CommitTo(req.LeaderCommitIndex); err != nil {

			return proto.AppendEntryResponse{
				Term:             raft.Term,
				Success:          false,
				ServerID:         raft.ServerID,
				PreviousLogIndex: raft.LogObj.LastIndex(),
				ExpectedIndex:    raft.LogObj.LastIndex() + 1,
			}, stepDown
		} else {
			//Need to execute the newly committed entries onto the state machine
			logEntries, _, _ := raft.LogObj.EntriesAfter(lastCommitIndex)
			newCommitIndex, _ := raft.LogObj.CommitInfo()

			for _, entry := range logEntries {
				if entry.Lsn() <= newCommitIndex {
					raft.CommitCh <- entry
				} else {
					break
				}
			}
		}
	}

	return proto.AppendEntryResponse{
		Term:             raft.Term,
		Success:          true,
		ServerID:         raft.ServerID,
		PreviousLogIndex: raft.LogObj.LastIndex(),
		ExpectedIndex:    raft.LogObj.LastIndex() + 1,
	}, stepDown
}

func (raft *Raft) sendHeartbeat() {
	timer := time.NewTimer(time.Duration(heartbeatMsgInterval) * time.Millisecond)
	for raft.running.Get() {
		if raft.CurrentState == LEADER {
			for _, server := range raft.ClusterConfig.Servers {
				if server.Id != raft.ServerID {

					if raft.CurrentState == LEADER {
						timer.Reset(time.Duration(heartbeatMsgInterval) * time.Millisecond)
						logEntries, _, previousLogEntry := raft.LogObj.EntriesAfter(raft.nextIndex[server.Id] - 1)
						prevLogIndex := raft.LogObj.LastIndex()
						prevLogTerm := raft.LogObj.LastTerm()
						if previousLogEntry != nil {
							prevLogIndex = previousLogEntry.Lsn()
							prevLogTerm = previousLogEntry.CurrentTerm()
						}

						heartbeatMsg := proto.AppendEntryRequest{
							LeaderID:          raft.ServerID,
							PreviousLogIndex:  prevLogIndex,
							PreviousLogTerm:   prevLogTerm,
							LeaderCommitIndex: raft.LogObj.GetCommitIndex(),
							Term:              raft.Term,
							LogEntries:        logEntries,
						}

						message := &proto.Event{proto.TypeHeartBeat, heartbeatMsg}

						go raft.sendToServerReplica(message, server.Id)
						log.Printf("At server %d, hearbeat sent to server %d", raft.ServerID, server.Id)
					}
				}
			}
			//Wait for hearbeat timeout
			<-timer.C
		} else {
			break
		}
	}
}

func getRandomWaitDuration() time.Duration {
	//Perfrom random selection for timeout peroid
	randomVal := rand.Intn(int(maxTimeout-minTimeout)) + minTimeout
	return time.Duration(randomVal) * time.Millisecond
}

func (raft *Raft) createReplicaConnections() {
	for _, server := range raft.ClusterConfig.Servers {
		//if server.Id == raft.ServerID {
		//	continue
		//} else {
		//	go raft.connect(server.Id, server.Hostname, server.LogPort)
		//}
		go raft.connect(server.Id, server.Hostname, server.LogPort)
	}
}

func (raft *Raft) connect(serverID int, hostname string, port int) {

	for raft.running.Get() {
		conn, err := net.Dial("tcp", hostname+":"+strconv.Itoa(port))
		if err != nil {
			//	log.Println("At server " + strconv.Itoa(raft.ServerID) + ", connect error " + err.Error())
			//wait for sometime before reattempting new connection

		} else {
			encoder := gob.NewEncoder(conn)
			raft.ReplicaChannels[serverID] = encoder
			break
		}
	}
}

func (raft *Raft) startListeningForReplicaEvents() {
	serverConfig := raft.ClusterConfig.Servers[0]
	for _, server := range raft.ClusterConfig.Servers {
		if server.Id == raft.ServerID {
			serverConfig = server
		}
	}

	psock, err := net.Listen("tcp", ":"+strconv.Itoa(serverConfig.LogPort))
	if err != nil {
		return
	}
	raft.eventListener = psock
	for raft.running.Get() {
		conn, err := raft.eventListener.Accept()
		if err != nil {
			return
		}
		go raft.RequestHandler(conn)
	}
}

func (raft *Raft) RequestHandler(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	for raft.running.Get() {
		event := new(proto.Event)
		err := decoder.Decode(&event)
		if err != nil {
			log.Println("Error Socket: " + err.Error())
			//TODO: handle
			break
		}
		raft.EventInCh <- *event
	}
}

func (raft *Raft) sendVoteRequest(voteRequestMsg *proto.VoteRequest) {
	voteRequestEvent := proto.Event{
		Type: proto.TypeVoteRequest,
		Data: voteRequestMsg,
	}

	for serverID, replicaSocket := range raft.ReplicaChannels {
		if replicaSocket != nil {
			replicaSocket.Encode(&voteRequestEvent)
			continue
		}
		log.Printf("Invalid channel to server" + strconv.Itoa(serverID))
	}
}

func (raft *Raft) sendVoteReply(voteReplyMsg *proto.VoteReply, serverID int) {
	voteReplyEvent := proto.Event{
		Type: proto.TypeVoteReply,
		Data: voteReplyMsg,
	}

	replicaSocket := raft.ReplicaChannels[serverID]
	if replicaSocket != nil {
		replicaSocket.Encode(&voteReplyEvent)
		return
	}

	log.Printf("Invalid channel to server" + strconv.Itoa(serverID))
}

func (raft *Raft) sendHeartBeatReply(heartBeartReplyMsg *proto.HeartBeatReply, serverID int) {
	heartBeartReplyEvent := proto.Event{
		Type: proto.TypeHeartBeatResponse,
		Data: heartBeartReplyMsg,
	}

	replicaSocket := raft.ReplicaChannels[serverID]
	if replicaSocket != nil {
		replicaSocket.Encode(&heartBeartReplyEvent)
		return
	}

	log.Printf("Invalid channel to server" + strconv.Itoa(serverID))
}

func (raft *Raft) sendToServerReplica(message *proto.Event, replicaID int) {
	//From list of channels, find out the channel for #replicaID
	//and send out a message
	var err error
	if replicaID == proto.Broadcast {

		for serverID, replicaSocket := range raft.ReplicaChannels {
			//if serverID == raft.ServerID {
			//	continue
			//} else {
			//	if replicaSocket != nil {
			//		err = replicaSocket.Encode(message)
			//	} else {
			//		err = errors.New("Invalid channel to server" + strconv.Itoa(serverID))
			//	}

			//	if err != nil {
			//		//log.Printf("At server %d, Send error - %s", raft.ServerID, err.Error())
			//		//log.Printf("At server %d, Attempting reconnect with server %d", raft.ServerID, serverID)

			//		serverConfig := raft.ClusterConfig.Servers[0]
			//		for _, server := range raft.ClusterConfig.Servers {
			//			if server.Id == serverID {
			//				serverConfig = server
			//			}
			//		}
			//		raft.connect(serverID, serverConfig.Hostname, serverConfig.LogPort)
			//		err = raft.ReplicaChannels[serverID].Encode(message)
			//		if message.Type == proto.TypeAppendEntryRequest {
			//			log.Printf("At server %d, message sent to server %d is %s", raft.ServerID, serverID, message)
			//		}
			//	}
			//}
			if replicaSocket != nil {
				err = replicaSocket.Encode(message)
			} else {
				err = errors.New("Invalid channel to server" + strconv.Itoa(serverID))
			}

			if err != nil {
				//log.Printf("At server %d, Send error - %s", raft.ServerID, err.Error())
				//log.Printf("At server %d, Attempting reconnect with server %d", raft.ServerID, serverID)

				serverConfig := raft.ClusterConfig.Servers[0]
				for _, server := range raft.ClusterConfig.Servers {
					if server.Id == serverID {
						serverConfig = server
					}
				}
				raft.connect(serverID, serverConfig.Hostname, serverConfig.LogPort)
				err = raft.ReplicaChannels[serverID].Encode(message)
				if message.Type == proto.TypeAppendEntryRequest {
					log.Printf("At server %d, message sent to server %d is %s", raft.ServerID, serverID, message)
				}
			}

		}
	} else {
		replicaSocket := raft.ReplicaChannels[replicaID]
		if replicaSocket != nil {
			err = replicaSocket.Encode(message)
		} else {
			err = errors.New("Invalid channel to server" + strconv.Itoa(replicaID))
		}
		if err != nil {
			//log.Printf("At server %d, Send error - %s", raft.ServerID, err.Error())
			//log.Printf("At server %d, Attempting reconnect with server %d", raft.ServerID, replicaID)

			serverConfig := raft.ClusterConfig.Servers[0]
			for _, server := range raft.ClusterConfig.Servers {
				if server.Id == replicaID {
					serverConfig = server
				}
			}
			raft.connect(replicaID, serverConfig.Hostname, serverConfig.LogPort)
			err = raft.ReplicaChannels[replicaID].Encode(message)
			if message.Type == proto.TypeAppendEntryRequest {
				log.Printf("At server %d, message sent to server %d is %s", raft.ServerID, replicaID, message)
			}
		}
	}
}

func (raft *Raft) StopServer() {
	log.Printf("At server %d, command for server stop received. Stopping the server.", raft.ServerID)
	raft.running.Set(false)
	if raft.eventListener != nil {
		raft.eventListener.Close()
	}
}
