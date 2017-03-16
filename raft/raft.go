package raft

import (
	"encoding/gob"
	"errors"
	util "github.com/pwzgorilla/miniraft/util"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
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
	Append(data []byte) (util.LogEntry, error)
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
	CommitCh      chan util.LogEntry
	LeaderID      int
	CurrentState  string //state of the server

	//entries for implementing Shared Log
	LogObj               *util.Log
	EventInCh            chan util.Event
	ServerQuitCh         chan chan struct{}
	ElectionTimer        <-chan time.Time
	Term                 uint64
	LastVotedTerm        uint64
	LastVotedCandidateID int
	ReplicaChannels      map[int]*gob.Encoder //map of replica id to socket

	timer *time.Timer

	running *util.AtomicBool

	//Fields required in case the server is leader
	nextIndex  map[int]util.Lsn
	matchIndex map[int]util.Lsn

	eventListener net.Listener
	Mutex         *sync.Mutex
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan util.LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.ClusterConfig = config
	raft.ServerID = thisServerId
	raft.CommitCh = commitCh
	//	raft.AppendRequestCh = make(chan handler.AppendRequestMessage)
	//	raft.LogEntryBuffer = make([]LogEntry, 0)
	raft.LastVotedCandidateID = -1
	raft.LastVotedTerm = 0
	raft.ReplicaChannels = make(map[int]*gob.Encoder)
	raft.nextIndex = make(map[int]util.Lsn)
	raft.matchIndex = make(map[int]util.Lsn)

	raft.LogObj = util.NewLog(raft.ServerID) //State the file name
	raft.LogObj.SendToStateMachine = func(entry *util.LogEntryObj) {
		raft.CommitCh <- *entry
	}
	raft.LogObj.FirstRead()
	raft.Term = raft.LogObj.LastTerm()

	//raft.timer = time.NewTimer(getRandomWaitDuration())

	raft.running = new(util.AtomicBool)
	raft.Mutex = &sync.Mutex{}
	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func (raft *Raft) Append(data []byte) (util.LogEntry, error) {
	if len(data) > 0 {
		var logEntry util.LogEntryObj
		logEntry = util.LogEntryObj{raft.LogObj.LastIndex() + 1, data, false, raft.Term}
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
				case util.TypeClientAppendRequest:
					// Do not handle clients in follower mode. Send it back up the
					// pipe with committed = false
					message := event.Data.(util.ClientAppendRequest)
					cmd, _ := util.DecodeCommand(message.Data)
					if cmd.Action == util.StopServer {
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

				case util.TypeVoteRequest:
					message := event.Data.(util.VoteRequest)

					voteResp, _ := raft.validateVoteRequest(message)
					go raft.sendToServerReplica(&util.Event{util.TypeVoteReply, voteResp}, message.CandidateID)
					raft.LeaderID = message.CandidateID
					raft.LastVotedCandidateID = message.CandidateID
					raft.LastVotedTerm = message.Term

				case util.TypeHeartBeat:
					message := event.Data.(util.AppendEntryRequest)
					log.Printf("At Server %d, received HeartBeat meassage from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					raft.timer.Reset(getRandomWaitDuration())

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending HeartBeat response to leader %d. Expected index is %d", raft.ServerID, message.LeaderID, resp.ExpectedIndex)

					go raft.sendToServerReplica(&util.Event{util.TypeHeartBeatResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case util.TypeAppendEntryRequest:
					message := event.Data.(util.AppendEntryRequest)
					log.Printf("At Server %d, received AppendEntryResquest from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending AppendEntryResponse to leader %d", raft.ServerID, message.LeaderID)

					go raft.sendToServerReplica(&util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case util.TypeTimeout:

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
	voteRequestMsg := util.VoteRequest{
		Term:         raft.Term,
		CandidateID:  raft.ServerID,
		LastLogIndex: raft.LogObj.LastIndex(),
		LastLogTerm:  raft.LogObj.LastTerm(),
	}

	go func() {
		log.Printf("Server %d send VoteMsg to others", raft.ServerID)
		raft.sendVoteMsg(&voteRequestMsg)
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
				case util.TypeClientAppendRequest:
					// Do not handle clients in follower mode. Send it back up the
					// pipe with committed = false
					message := event.Data.(util.ClientAppendRequest)

					cmd, _ := util.DecodeCommand(message.Data)
					if cmd.Action == util.StopServer {
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

				case util.TypeVoteRequest:
					message := event.Data.(util.VoteRequest)
					log.Println("Server ", raft.ServerID, " Received vote request from", message.CandidateID)
					resp, changeState := raft.validateVoteRequest(message)

					go raft.sendToServerReplica(&util.Event{util.TypeVoteReply, resp}, message.CandidateID)
					if changeState {
						raft.LeaderID = -1 //TODO: changed from  -1 to message.CandidateID
						raft.LastVotedCandidateID = message.CandidateID
						raft.LastVotedTerm = message.Term
						log.Println("Server ", raft.ServerID, " changing state to ", FOLLOWER)
						return FOLLOWER
					}

				case util.TypeVoteReply:
					message := event.Data.(util.VoteReply)
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

				case util.TypeHeartBeat:
					message := event.Data.(util.AppendEntryRequest)
					log.Printf("At Server %d, received HeartBeat meassage from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}
					raft.timer.Reset(getRandomWaitDuration())

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending HeartBeat response to leader %d", raft.ServerID, message.LeaderID)

					go raft.sendToServerReplica(&util.Event{util.TypeHeartBeatResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case util.TypeAppendEntryRequest:
					message := event.Data.(util.AppendEntryRequest)
					log.Printf("At Server %d, received AppendEntryResquest from %d", raft.ServerID, message.LeaderID)

					if raft.LeaderID == -1 {
						raft.LeaderID = message.LeaderID
						log.Printf("At server %d, found a new leader %d", raft.ServerID, message.LeaderID)
					}

					resp, changeOfLeader := raft.validateAppendEntryRequest(message)

					log.Printf("At Server %d, sending AppendEntryResponse to leader %d", raft.ServerID, message.LeaderID)

					go raft.sendToServerReplica(&util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)

					if changeOfLeader {
						raft.LeaderID = message.LeaderID
						return FOLLOWER
					}

				case util.TypeTimeout:

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
	var previousLogEntryForConsensus util.LogEntry
	var messagePendingForConsensus util.Event

	for raft.running.Get() {
		select {
		case event := <-raft.EventInCh:
			switch event.Type {
			case util.TypeClientAppendRequest:

				log.Printf("At Server %d, received client append request", raft.ServerID)
				appendRPCMessage := util.AppendEntryRequest{
					LeaderID:          raft.ServerID,
					LeaderCommitIndex: raft.LogObj.GetCommitIndex(),
					PreviousLogIndex:  raft.LogObj.LastIndex(),
					Term:              raft.Term,
					PreviousLogTerm:   raft.LogObj.LastTerm(),
				}

				message := event.Data.(util.ClientAppendRequest)
				cmd, _ := util.DecodeCommand(message.Data)
				if cmd.Action == util.StopServer {
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
				util.ResponseChannelStore.Lock()
				util.ResponseChannelStore.M[logEntry.Lsn()] = message.ResponseCh
				util.ResponseChannelStore.Unlock()

				previousLogEntryForConsensus = logEntry

				//now check for consensus
				appendRPCMessage.LogEntries = make([]util.LogEntryObj, 0)
				appendRPCMessage.LogEntries = append(appendRPCMessage.LogEntries, logEntry.(util.LogEntryObj))

				log.Printf("At Server %d, sending TypeAppendEntryRequest", raft.ServerID)

				messagePendingForConsensus = util.Event{util.TypeAppendEntryRequest, appendRPCMessage}
				go raft.sendToServerReplica(&messagePendingForConsensus, util.Broadcast)

			case util.TypeVoteRequest:

				message := event.Data.(util.VoteRequest)
				resp, changeState := raft.validateVoteRequest(message)

				go raft.sendToServerReplica(&util.Event{util.TypeVoteReply, resp}, message.CandidateID)
				if changeState {
					log.Printf("At Server %d, change of leader from %d to %d\n", raft.ServerID, raft.LeaderID, message.CandidateID)
					raft.LeaderID = message.CandidateID //TODO: change from -1 to
					return FOLLOWER
				}

			case util.TypeAppendEntryRequest:
				message := event.Data.(util.AppendEntryRequest)
				log.Printf("At server %d, Error - Two servers in leader state found. Server %d at term %d, Server %d at term %d", raft.ServerID, raft.ServerID, raft.Term, message.LeaderID, message.Term)
				resp, changeState := raft.validateAppendEntryRequest(message)

				if message.LogEntries != nil {
					//send serponse to the leader
					go raft.sendToServerReplica(&util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)
				}

				if changeState {
					raft.LeaderID = message.LeaderID
					log.Printf("At server %d, Changing state to FOLLOWER", raft.ServerID)
					return FOLLOWER
				}

			case util.TypeHeartBeatResponse:
				message := event.Data.(util.AppendEntryResponse)
				raft.nextIndex[message.ServerID] = message.ExpectedIndex

			case util.TypeAppendEntryResponse:
				// Do not handle clients in follower mode. Send it back up the
				// pipe with committed = false
				message := event.Data.(util.AppendEntryResponse)
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
								util.ResponseChannelStore.RLock()
								responseChannel := util.ResponseChannelStore.M[previousLogEntryForConsensus.Lsn()]
								util.ResponseChannelStore.RUnlock()

								if responseChannel == nil {
									log.Printf("At server %d, Response channel for LogEntry with lsn %d not found", raft.ServerID, int(previousLogEntryForConsensus.Lsn()))
								} else {
									//Delete the entry for response channel handle
									util.ResponseChannelStore.Lock()
									delete(util.ResponseChannelStore.M, previousLogEntryForConsensus.Lsn())
									util.ResponseChannelStore.Unlock()
									*responseChannel <- "ERR_QUORUM_NOT_ACHIEVED\r\n"
								}
							*/

							//wait for some time before making a new request
							<-timer.C

							//reset ackCount and nakCount
							nakCount = 0
							ackCount = 0
							go raft.sendToServerReplica(&messagePendingForConsensus, util.Broadcast)
						}

					}
				} else {
					log.Printf("At Server %d, received AppendEntryResponse for older term (%d>%d)", raft.ServerID, raft.Term, message.Term)
				}

			case util.TypeTimeout:
			}
		case <-raft.timer.C:
		}
	}
	return raft.CurrentState

}

func (raft *Raft) InitServer() {
	//register for RPC
	gob.Register(util.LogEntryObj{})
	gob.Register(util.Event{})
	gob.Register(util.VoteRequest{})
	gob.Register(util.VoteReply{})
	gob.Register(util.AppendEntryRequest{})
	gob.Register(util.AppendEntryResponse{})
	gob.Register(util.ClientAppendRequest{})
	gob.Register(util.Timeout{})

	go raft.startListeningForReplicaEvents()

	go raft.createReplicaConnections()

	raft.running.Set(true)
	raft.Loop()
}

func (raft *Raft) validateVoteRequest(req util.VoteRequest) (util.VoteReply, bool) {
	if req.Term <= raft.Term {
		return util.VoteReply{raft.Term, false, raft.ServerID}, false
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
		return util.VoteReply{raft.Term, false, raft.ServerID}, changeState
	} else {
		raft.LastVotedCandidateID = req.CandidateID
		raft.timer.Reset(getRandomWaitDuration())
		log.Printf("At Server %d, sending positive vote reply to server %d", raft.ServerID, req.CandidateID)
		return util.VoteReply{raft.Term, true, raft.ServerID}, changeState
	}

}

func (raft *Raft) validateAppendEntryRequest(req util.AppendEntryRequest) (util.AppendEntryResponse, bool) {
	expectedIndex := raft.LogObj.LastIndex() + 1
	if raft.LogObj.LastIndex() == 0 {
		expectedIndex = 1
	}

	if req.Term < raft.Term {
		return util.AppendEntryResponse{
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
		return util.AppendEntryResponse{
			Term:             raft.Term,
			Success:          false,
			ServerID:         raft.ServerID,
			PreviousLogIndex: raft.LogObj.LastIndex(),
			ExpectedIndex:    expectedIndex,
		}, stepDown
	}

	var entry util.LogEntry
	if req.LogEntries != nil && len(req.LogEntries) == 1 {
		entry = req.LogEntries[0]
		if entry != nil {
			// Append entry to the log
			if err := raft.LogObj.AppendEntry(entry.(util.LogEntryObj)); err != nil {
				log.Printf("At Server %d, Log Append failed - %s", raft.ServerID, err.Error())

				return util.AppendEntryResponse{
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

			return util.AppendEntryResponse{
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

			return util.AppendEntryResponse{
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

	return util.AppendEntryResponse{
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

						heartbeatMsg := util.AppendEntryRequest{
							LeaderID:          raft.ServerID,
							PreviousLogIndex:  prevLogIndex,
							PreviousLogTerm:   prevLogTerm,
							LeaderCommitIndex: raft.LogObj.GetCommitIndex(),
							Term:              raft.Term,
							LogEntries:        logEntries,
						}

						message := &util.Event{util.TypeHeartBeat, heartbeatMsg}

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
		event := new(util.Event)
		err := decoder.Decode(&event)
		if err != nil {
			log.Println("Error Socket: " + err.Error())
			//TODO: handle
			break
		}
		raft.EventInCh <- *event
	}
}

func (raft *Raft) sendVoteMsg(voteMsg *util.VoteRequest) {
	voteMsgEvent := util.Event{
		Type: util.TypeVoteRequest,
		Data: voteMsg,
	}

	for serverID, replicaSocket := range raft.ReplicaChannels {
		if replicaSocket != nil {
			replicaSocket.Encode(&voteMsgEvent)
		}
		log.Printf("Invalid channel to server" + strconv.Itoa(serverID))
	}
}

func (raft *Raft) sendToServerReplica(message *util.Event, replicaID int) {
	//From list of channels, find out the channel for #replicaID
	//and send out a message
	var err error
	if replicaID == util.Broadcast {

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
			//		if message.Type == util.TypeAppendEntryRequest {
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
				if message.Type == util.TypeAppendEntryRequest {
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
			if message.Type == util.TypeAppendEntryRequest {
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
