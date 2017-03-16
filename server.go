package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	handler "github.com/saurabh-hote/cs733/assignment-3/handler"
	raft "github.com/saurabh-hote/cs733/assignment-3/raft"
	util "github.com/saurabh-hote/cs733/assignment-3/util"
	"io/ioutil"
	"log"
)

type NullWriter int

func (NullWriter) Write([]byte) (int, error) {
	return 0, nil
}

func ReadConfig(path string) (*raft.ClusterConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf raft.ClusterConfig
	err = json.Unmarshal(data, &conf)
	if err == nil && len(conf.Servers) < 1 {
		err = errors.New("No Server Configuration found")
	}
	return &conf, err
}

func main() {
	fmt.Println("======", "start")
	//log.SetOutput(new(NullWriter))

	//TODO: Read the config.json file to get all the server configurations
	clusterConfig, err := ReadConfig("config.json")
	if err != nil {
		log.Println("Error parsing config file : ", err.Error())
		return
	}

	//starting the server
	serverIdPtr := flag.Int("id", 1, "an int")
	flag.Parse()

	if err != nil {
		log.Println("Invalid Server ID provided : ", err.Error())
	}
	log.Println("Starting sevrer with ID ", *serverIdPtr)

	commitCh := make(chan util.LogEntry, 10000)
	raftInstance, err := raft.NewRaft(clusterConfig, *serverIdPtr, commitCh)
	if err != nil {
		log.Println("Error creating server instance : ", err.Error())
	}

	raftInstance.EventInCh = make(chan util.Event, 1000)

	//First entry in the ClusterConfig will be the default leader
	var clientPort int
	for _, server := range raftInstance.ClusterConfig.Servers {
		//Initialize the connection handler module
		if server.Id == raftInstance.ServerID {
			clientPort = server.ClientPort
			raftInstance.CurrentState = raft.FOLLOWER
		}
	}

	if clientPort <= 0 {
		log.Println("Server's client port not valid")
	} else {
		go handler.StartConnectionHandler(raftInstance.ServerID, clientPort, raftInstance.EventInCh, raftInstance.Mutex)
	}

	//Inititialize the KV Store Module
	go raft.InitializeKVStore(raftInstance.ServerID, raftInstance.CommitCh)

	//Initialize the server
	raftInstance.InitServer()

	log.Println("Started raft Instance for server ID ", raftInstance.ServerID)
}
