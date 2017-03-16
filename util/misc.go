package util

import (
	"sync"
	"bytes"
	"encoding/gob"
	"sync/atomic"
)
 
type AtomicBool struct {flag int32}
 
func (b *AtomicBool) Set(value bool) {
  var i int32 = 0
  if value {i = 1}
  atomic.StoreInt32(&(b.flag), int32(i))
}
 
func (b *AtomicBool) Get() bool {
  if atomic.LoadInt32(&(b.flag)) != 0 {return true}
  return false
}
 
var ResponseChannelStore = struct {
	sync.RWMutex
	M map[Lsn]*chan string
}{M: make(map[Lsn]*chan string)}


const (
	Set = iota
	Get
	Getm
	Cas
	Delete
	Cleanup
	StopServer
)

type Command struct {
	Action   int
	Key      string
	Expiry   int64
	Version  uint64
	Numbytes int
	Data     []byte
}

//Encode using gob
func EncodeCommand(cmd Command) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(cmd)
	return buff.Bytes(), err
}

//Decode using gob
func DecodeCommand(data []byte) (Command, error) {
	var buff bytes.Buffer
	buff.Write(data)
	enc := gob.NewDecoder(&buff)
	var command Command
	err := enc.Decode(&command)
	return command, err
}