package raft

import (
	"container/heap"
	"fmt"
	util "github.com/pwzgorilla/miniraft/util"
	"log"
	"time"
)

type command util.Command

type value struct {
	data     []byte
	numbytes int
	version  uint64
	expiry   int64
}

//Map Manager
func InitializeKVStore(serverID int, ch chan util.LogEntry) { //	This channel has to be of type MyLogEntry. This is commitCh
	//The map which actually stores values
	m := make(map[string]value)
	h := &nodeHeap{}
	const heapCleanupInterval = 60
	go cleaner(heapCleanupInterval, ch)
	for {
		logEntry := <-ch
		cmd, _ := util.DecodeCommand(logEntry.Data())

		responseMsg := "ERR_NOT_FOUND\r\n"
		val, ok := m[cmd.Key]

		switch cmd.Action {
		case util.Set:
			{
				var version uint64
				version = 0
				if ok {
					version = val.version
				}

				t := cmd.Expiry
				if t != 0 {
					t += time.Now().Unix()
				}
				m[cmd.Key] = value{cmd.Data, cmd.Numbytes, version, t}
				if cmd.Expiry != 0 {
					heap.Push(h, node{t, cmd.Key, version})
				}
				responseMsg = fmt.Sprintf("OK %v\r\n", version)
			}
		case util.Get:
			{
				if ok {
					responseMsg = fmt.Sprintf("VALUE %v\r\n"+string(val.data)+"\r\n", val.numbytes)
				}
			}
		case util.Getm:
			{
				if ok {
					t := val.expiry
					if t != 0 {
						t = val.expiry - time.Now().Unix() // remaining time
					}
					responseMsg = fmt.Sprintf("VALUE %v %v %v\r\n"+string(val.data)+"\r\n", val.version, t, val.numbytes)
				}
			}
		case util.Cas:
			{
				if ok {
					if val.version == cmd.Version {
						t := cmd.Expiry
						if t != 0 {
							t += time.Now().Unix()
						}
						version := val.version + 1
						m[cmd.Key] = value{cmd.Data, cmd.Numbytes, version, t}
						if cmd.Expiry != 0 {
							heap.Push(h, node{t, cmd.Key, version})
						}
						responseMsg = fmt.Sprintf("OK %v\r\n", version)
					} else {
						responseMsg = fmt.Sprintf("ERR_VERSION\r\n")
					}
				}
			}
		case util.Delete:
			{
				if ok {
					delete(m, cmd.Key)
					responseMsg = "DELETED\r\n"
				}
			}
		case util.Cleanup:
			{
				t := time.Now().Unix()
				for (*h).Len() != 0 && (*h)[0].expiry <= t {
					root := heap.Pop(h).(node)
					v, e := m[root.key]
					if e && root.version == v.version {
						delete(m, root.key)
					}
				}
				responseMsg = "CLEANED\r\n"
			}
		default:
			{
				responseMsg = "ERR_INTERNAL\r\n"
			}
		}

		if cmd.Action != util.Cleanup {
			// Send response to appropriate handler's channel
			util.ResponseChannelStore.RLock()
			responseChannel := util.ResponseChannelStore.M[logEntry.Lsn()]
			util.ResponseChannelStore.RUnlock()

			if responseChannel == nil {
				log.Printf("At server %d,  Response channel for LogEntry not found", serverID)
			} else {
				//Delete the entry for response channel handle
				util.ResponseChannelStore.Lock()
				delete(util.ResponseChannelStore.M, logEntry.Lsn())
				util.ResponseChannelStore.Unlock()
				*responseChannel <- responseMsg
			}
		}
	}
}

func cleaner(interval int, ch chan util.LogEntry) {
	command := util.Command{util.Cleanup, "", 0, 0, 0, nil}
	data, err := util.EncodeCommand(command)
	logEntry := util.LogEntryObj{0, data, false, 0}
	if err != nil {
		log.Println("Error encoding the command ", err.Error())
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for range ticker.C {
		ch <- logEntry
	}
}
