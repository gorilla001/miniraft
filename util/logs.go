package util

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"strconv"
	"sync"
)

type Log struct {
	sync.RWMutex
	SendToStateMachine func(*LogEntryObj)
	db                 *bolt.DB
	entries            []LogEntryObj
	commitIndex        Lsn
	initialTerm        uint64
}

// create new log
func NewLog(serverID int) *Log {
	boltDB, err := bolt.Open("tmp/log"+strconv.Itoa(serverID)+".db ", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	logObj := &Log{
		entries:     []LogEntryObj{},
		db:          boltDB,
		commitIndex: 0,
		initialTerm: 0,
	}

	err = logObj.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("LogBucket"))
		if bucket == nil {
			_, err = tx.CreateBucket([]byte("LogBucket"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return logObj
}

//Returnt he current log index
func (logObj *Log) CurrentIndex() Lsn {
	if len(logObj.entries) == 0 {
		return 0
	}
	return logObj.entries[len(logObj.entries)-1].Lsn()
}

// Closes the log database.
func (logObj *Log) Close() {
	logObj.Lock()
	defer logObj.Unlock()

	logObj.db.Close()
	logObj.entries = make([]LogEntryObj, 0)
}

//Does log contains the retry with perticular index and term
func (logObj *Log) ContainsEntry(index uint64, term uint64) bool {
	entry := logObj.GetEntry(index)
	return (entry != nil && entry.CurrentTerm() == term)
}

//get perticular entry by index
func (logObj *Log) GetEntry(index uint64) LogEntry {
	if index <= 0 || index > (uint64(len(logObj.entries))) {
		return nil
	}
	return logObj.entries[index-1]
}

//read all enteries from disk when log intialized
func (logObj *Log) FirstRead() error {
	logObj.entries = []LogEntryObj{}

	err := logObj.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("LogBucket"))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			entry := new(LogEntryObj)
			b := bytes.NewBufferString(string(v))
			dec := gob.NewDecoder(b)
			err := dec.Decode(entry)
			if err != nil {
				log.Printf("Entry decode error - %s", err.Error())
			}

			if entry.Lsn() > 0 {
				// Append entry.
				logObj.entries = append(logObj.entries, *entry)
				//TODO: changed the code for incrementing commit index
				/*if entry.Lsn() <= logObj.commitIndex {
					logObj.SendToStateMachine(entry)
				}
				*/

				if entry.IsCommitted() {
					log.Printf("Committed entry found with lsn - %d", entry.Lsn())

					logObj.SendToStateMachine(entry)
					logObj.commitIndex = entry.Lsn()
				} else {
					log.Printf("non Committed entry found with lsn - %d", entry.Lsn())
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("Log Read Error - %s", err.Error())
	}
	return nil
}

//It will return the entries after the given index
func (logObj *Log) EntriesAfter(index Lsn) ([]LogEntryObj, uint64, LogEntry) {

	if index <= 0 {
		return nil, 0, nil
	}
	lastIndex := logObj.LastIndex()

	if index > (lastIndex - 1) {
		///		log.Printf("LogObj: Index is beyond end of log: %v %v", lastIndex, index)
		return nil, 0, nil
	}

	pos := Lsn(0)
	lastTerm := uint64(0)
	var previousLogEntry LogEntry
	for ; pos < lastIndex; pos++ {
		if logObj.entries[pos].Lsn() > index {
			break
		} else if logObj.entries[pos].Lsn() == index {
			previousLogEntry = logObj.entries[pos]
		}
		lastTerm = logObj.entries[pos].CurrentTerm()
	}

	entries := logObj.entries[pos:]
	result := []LogEntryObj{}

	for _, entry := range entries {
		result = append(result, entry)
	}

	//if entries are less then max limit then return all entries
	return result, lastTerm, previousLogEntry
}

//Return the last log entry term
func (logObj *Log) LastTerm() uint64 {
	if len(logObj.entries) <= 0 {
		return 0
	}
	return logObj.entries[len(logObj.entries)-1].CurrentTerm()
}

//Remove the enteries which are not commited
func (logObj *Log) Discard(index Lsn, term uint64) error {
	logObj.Lock()
	defer logObj.Unlock()
	if index == 0 {
		return nil
	} else if index < 0 || index < logObj.commitIndex {
		return errors.New("Invalid Index")
	} else if index <= Lsn(len(logObj.entries)) {
		// Do not discard if the entry at index does not have the matching term.
		logEntry := logObj.entries[index-1]
		if logEntry.CurrentTerm() > term {
			return errors.New("Discard failed. Term mismatch")
		} else if logEntry.IsCommitted() {
			return errors.New("Discard failed. Entry already committed.")
		} else if index < Lsn(len(logObj.entries)) {

			buf := make([]byte, 8)

			// notify clients if this node is the previous leader
			for i := uint64(index); i < uint64(len(logObj.entries)); i++ {
				entry := logObj.entries[i]
				binary.LittleEndian.PutUint64(buf, uint64(entry.Lsn()))

				err := logObj.db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("LogBucket"))
					err := b.Delete(buf)
					return err
				})

				if err != nil {
					log.Printf("Entry with lsn %d not found in db", entry.Lsn())
				}
				//entry.IsCommitted() = false
			}
			logObj.entries = logObj.entries[0:index]
		}
	} else {
		return errors.New("Discard failed. Index out of range.")
	}
	return nil
}

//Return lastest commit index
func (logObj *Log) GetCommitIndex() Lsn {
	return logObj.commitIndex
}

//Return lastlog entry index
func (logObj *Log) LastIndex() Lsn {
	if len(logObj.entries) <= 0 {
		return 0
	}
	return logObj.entries[len(logObj.entries)-1].Lsn()
}

// Appends a series of entries to the log.
func (logObj *Log) AppendEntries(entries []LogEntryObj) error {
	logObj.Lock()
	defer logObj.Unlock()

	// Append each entry but exit if we hit an error.
	for _, entry := range entries {
		if err := logObj.writeToDB(&entry); err != nil {
			return err
		} else {
			logObj.entries = append(logObj.entries, entry)
		}
	}

	return nil
}

func (logObj *Log) AppendEntry(entry LogEntryObj) error {
	logObj.Lock()
	defer logObj.Unlock()

	if len(logObj.entries) > 0 {
		if entry.CurrentTerm() < logObj.LastTerm() {
			return errors.New("AppendEntry failed. Invalid term")
		}
		if entry.CurrentTerm() == logObj.LastTerm() && entry.Lsn() <= logObj.LastIndex() {
			return errors.New("AppendEntry failed. Invalid index")
		}
	}

	if err := logObj.writeToDB(&entry); err != nil {
		return err
	} else {
		logObj.entries = append(logObj.entries, entry)
	}
	return nil

}

//Update commit index
func (logObj *Log) UpdateCommitIndex(index Lsn) {
	logObj.Lock()
	defer logObj.Unlock()

	if index > logObj.commitIndex {
		logObj.commitIndex = index
	}

}

//Commit current log to given index
func (logObj *Log) CommitTo(commitIndex Lsn) error {
	logObj.Lock()
	defer logObj.Unlock()

	if commitIndex > Lsn(len(logObj.entries)) {
		commitIndex = Lsn(len(logObj.entries))
	}
	if commitIndex < logObj.commitIndex {
		return nil
	}

	for i := logObj.commitIndex + 1; i <= commitIndex; i++ {
		entryIndex := i - 1
		entry := logObj.entries[entryIndex]
		entry.Committed = true

		// Update commit index.
		logObj.commitIndex = entry.Lsn()

		var data bytes.Buffer
		enc := gob.NewEncoder(&data)
		err := enc.Encode(entry)
		if err != nil {
			log.Printf("GOB error: %s", err.Error())
		}

		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(entry.Lsn()))

		err = logObj.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("LogBucket"))
			err := b.Put(buf, data.Bytes())
			return err
		})

	}

	return nil
}

//Get last commit information
func (logObj *Log) CommitInfo() (index Lsn, term uint64) {
	if logObj.commitIndex == 0 {
		return 0, 0
	}

	if logObj.commitIndex == 0 {
		return 0, 0
	}

	entry := logObj.entries[logObj.commitIndex-1]
	return entry.Lsn(), entry.CurrentTerm()
}

//Write entry to leveldb
func (logObj *Log) writeToDB(logItem *LogEntryObj) error {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(logItem)
	if err != nil {
		log.Printf("GOB error: %s", err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64((*logItem).Lsn()))

	err = logObj.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("LogBucket"))
		err := b.Put(buf, data.Bytes())
		return err
	})
	return err

}
