package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	raft "github.com/saurabh-hote/cs733/assignment-3/raft"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

var cmdObjects = make(map[int]*exec.Cmd)
var cmdChannel = make(chan *exec.Cmd, 1)

const noOfClientThreads int = 1
const noOfRequestsPerThread int = 1
const leaderElectionTimeout = 10 * time.Second

var clusterConfig *raft.ClusterConfig

func init() {
	serverConfig := []raft.ServerConfig{
		{1, "localhost", 5001, 6001},
		{2, "localhost", 5002, 6002},
		{3, "localhost", 5003, 6003},
		{4, "localhost", 5004, 6004},
		{5, "localhost", 5005, 6005},
	}
	clusterConfig = &raft.ClusterConfig{"/log", serverConfig}

	data, _ := json.Marshal(*clusterConfig)
	ioutil.WriteFile("config.json", data, 0644)

	serverReplicas := len((*clusterConfig).Servers)

	for _, serverConfig := range clusterConfig.Servers {
		go startServer(serverConfig)
		cmdobj := <-cmdChannel
		cmdObjects[serverConfig.Id] = cmdobj
	}

	log.Println("Waiting for all server instances to start.........")
	for len(cmdObjects) < serverReplicas {
		time.Sleep(100 * time.Millisecond)
	}
	waitTimeForServerReplicaStart := 5 * time.Second
	time.Sleep(waitTimeForServerReplicaStart)
}

/*
Test for checking the the correct version
The test execytes multiple set commands and then checks for the correct version on getm execute
*/
func TestEntryVersion(t *testing.T) {
	//start the server
	//go main()
	leaderConfig, err := getLeaderConfig(t)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	} else {
		log.Printf("Leader found at server %d", leaderConfig.Id)
	}
	commands := []string{
		//"set saurabh 60 7\r\nHoteABC\r\n",
		"set saurabh 60 7\r\nHotePQR\r\n",
	}
	deleteCommand := "delete saurabh\r\n"
	verifyCommand := "getm saurabh\r\n"

	done := make(chan bool)

	//delete the key to test
	if executeDelete(t, deleteCommand, leaderConfig) {
		t.Error("Error executing command " + deleteCommand)
	}

	//spwan client threads
	connectionCounter := 0
	for connectionCounter < noOfClientThreads {
		go executeCommands(t, done, commands, 0, leaderConfig)
		connectionCounter += 1
	}
	connectionCounter = 0

	for connectionCounter < noOfClientThreads {
		<-done
		connectionCounter += 1
	}

	//now verify the version
	versionReceived := executeGetMForVersion(t, verifyCommand, leaderConfig)
	versionReceived++ //since the versioning starts from 0
	if noOfClientThreads*noOfRequestsPerThread == int(versionReceived) {
		t.Log("TestVersionMatch Passed.")
	} else {
		t.Log("TestVersionMatch Failed. Expected " + strconv.Itoa(noOfClientThreads*noOfRequestsPerThread) + " received " + strconv.FormatInt(versionReceived, 10))
	}
}

func TestSingleLeader(t *testing.T) {
	var err error
	verifyCommand := "getm saurabh\r\n"
	attempts := 0
	leaderFound := 0
	for {
		leaderFound = 0
		for _, server := range clusterConfig.Servers {
			version := executeGetMForVersion(t, verifyCommand, server)
			if version != -2 {
				leaderFound++
			}
		}
		attempts++

		if leaderFound >= 1 {
			break
		} else if attempts < 3 {
			timer := time.NewTimer(1 * time.Second)
			<-timer.C
		} else {
			err = errors.New("Leader not found after 3 attempts")
		}
	}
	if leaderFound > 1 {
		t.Log("Test for Single Raft Leader failed. More than one leader found")
		t.Fail()
	} else if err != nil {
		t.Log(err.Error())
		t.Fail()
	} else {
		log.Printf("Test for Single Raft leader passed.")
		t.Skip()
	}
}

func TestConsensus(t *testing.T) {
	log.Println("Finding the leader.........")
	leaderConfig, err := getLeaderConfig(t)
	if err != nil {
		t.Fail()
	} else {
		log.Printf("Leader found at server %d", leaderConfig.Id)
	}

	log.Printf("Killing all the other server replicas except the leader..")
	for _, server := range clusterConfig.Servers {
		if server.Id != leaderConfig.Id {
			stopServer(server)
			//Remove cmd from the map
			delete(cmdObjects, server.Id)
		}
	}

	log.Println("All server replicas killed. Now firing a command on leader.........")
	log.Println("This command is expected to block until the leader achieves quorum.........")
	doneCh := make(chan bool)
	go func(leaderID int) {
		log.Println("Waiting for few seconds before restarting replicas..")
		timer := time.NewTimer(5 * time.Second)
		<-timer.C
		log.Println("Now starting the replicas...")
		for _, server := range clusterConfig.Servers {
			if server.Id != leaderID {
				log.Printf("Starting server %d", server.Id)
				go startServer(server)
				cmdobj := <-cmdChannel
				cmdObjects[server.Id] = cmdobj
			}
		}
		doneCh <- true
	}(leaderConfig.Id)

	<- doneCh

	verifyCommand := "getm saurabh\r\n"
	version := executeGetMForVersion(t, verifyCommand, leaderConfig)
	if version != -2 {
		log.Printf("TestConsensus passed. Quorum achieved for the command at leader %d", leaderConfig.Id)
		t.Skip()
	} else {
		log.Printf("TestConsensus failed. Quorum not achieved for the command at leader %d", leaderConfig.Id)
		t.Fail()
	}
}

func TestKillLeader(t *testing.T) {
	log.Println("Finding the leader.........")
	leaderConfig, err := getLeaderConfig(t)
	if err != nil {
		t.Fail()
	} else {
		log.Printf("Leader found at server %d", leaderConfig.Id)
	}

	log.Printf("Killing the leader server %d", leaderConfig.Id)
	stopServer(leaderConfig)

	//Remove cmd from the map
	delete(cmdObjects, leaderConfig.Id)

	log.Println("Leader killed. Finding new leader now.........")

	leaderConfig, err = getLeaderConfig(t)
	if err != nil {
		t.Fail()
	} else {
		log.Printf("New leader found at server %d", leaderConfig.Id)
		t.Skip()
	}
}

func TestKillAllServers(t *testing.T) {
	log.Println("Waiting for all server instances to stop.........")

	for _, cmd := range cmdObjects {
		err := cmd.Process.Kill()
		if err != nil {
			log.Println("go routine Killing error" + err.Error())
		}

		cmd.Process.Wait()
	}
	log.Println("Killing server processes......")
	if runtime.GOOS == "windows" {
		exec.Command("taskkill", "/IM", "server.exe", "/F").Run()
	} else {
		exec.Command("pkill", "server").Run()
	}
	log.Println("All Server processes killed.")
}

//Miscellaneous functions
func executeCommands(t *testing.T, done chan bool, commands []string, execCount int, leaderConfig raft.ServerConfig) bool {
	conn, err := net.Dial("tcp", leaderConfig.Hostname+":"+strconv.Itoa(leaderConfig.ClientPort))
	log.Println("Conencted to server ", leaderConfig.Id)
	if err != nil {
		t.Error(err)
		done <- false
		return false
	}

	connbuf := bufio.NewReader(conn)
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	var line []byte
	result := true
	executionCount := noOfRequestsPerThread
	if execCount > 0 {
		executionCount = execCount
	}

	j := 0
	for j < executionCount {
		commandIndex := randomGenerator.Int() % len(commands)
		io.Copy(conn, bytes.NewBufferString(commands[commandIndex]))
		log.Printf("Sent \"%s\" to server %d", commands[commandIndex], leaderConfig.Id)

		if !strings.HasSuffix(strings.Split(commands[commandIndex], "\r\n")[0], "noreply") {

			line, err = connbuf.ReadBytes('\n')
			input := strings.TrimRight(string(line), "\r\n")
			log.Printf("Received \"%s\" from server %d", input, leaderConfig.Id)
			array := strings.Split(input, " ")

			if array[0] == "VALUE" {
				line, err = connbuf.ReadBytes('\n')
				input = strings.TrimRight(string(line), "\r\n")
				log.Println(input)

				if err != nil {
					result = false
					break
				}
			} else if array[0] != "OK" {
				result = false
				break
			}
		}
		j += 1
	}

	if err != nil {
		t.Error(err)
	}
	done <- result
	conn.Close()
	return result
}

func executeGetMForVersion(t *testing.T, command string, leaderConfig raft.ServerConfig) int64 {
	var version int64
	conn, err := net.Dial("tcp", leaderConfig.Hostname+":"+strconv.Itoa(leaderConfig.ClientPort))
	if err != nil {
		t.Log(err)
		return -1
	}

	connbuf := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString(command))
	log.Printf("Sent \"%s\" to server %d", command, leaderConfig.Id)

	line, _ := connbuf.ReadBytes('\n')
	input := strings.TrimRight(string(line), "\r\n")
	log.Printf("Received \"%s\" from server %d", input, leaderConfig.Id)

	array := strings.Split(input, " ")
	result := array[0]
	if result == "VALUE" {
		line, err = connbuf.ReadBytes('\n')
		version, _ = strconv.ParseInt(array[1], 10, 64)
		if err != nil {
			version = -1
		}
	} else if result == "ERR_REDIRECT" {
		version = -2
	} else {
		version = -1
	}

	if err != nil {
		t.Error(err)
	}
	conn.Close()
	return version
}

func executeDelete(t *testing.T, command string, leaderConfig raft.ServerConfig) bool {
	leaderConfig, err := getLeaderConfig(t)
	conn, err := net.Dial("tcp", leaderConfig.Hostname+":"+strconv.Itoa(leaderConfig.ClientPort))
	if err != nil {
		t.Error(err)
		return false
	}

	connbuf := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString(command))
	log.Printf("Sent \"%s\" to server %d", command, leaderConfig.Id)

	line, _ := connbuf.ReadBytes('\n')
	input := strings.TrimRight(string(line), "\r\n")
	log.Printf("Received \"%s\" from server %d", input, leaderConfig.Id)
	conn.Close()

	if input == "DELETED" {
		t.Skip("Delete Passed")
		return true

	} else {
		return false
	}
}

func startServer(serverConfig raft.ServerConfig) bool {
	cmd := exec.Command("go", "run", "server.go", "-id="+strconv.Itoa(serverConfig.Id))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmdChannel <- cmd
	cmd.Run()
	log.Printf("Server %d stopped", serverConfig.Id)
	return true
}

func stopServer(serverConfig raft.ServerConfig) bool {
	conn, err := net.Dial("tcp", serverConfig.Hostname+":"+strconv.Itoa(serverConfig.ClientPort))
	if err != nil {
		log.Printf("Failed to execute the command to stop server %d", serverConfig.Id)
		return true
	}
	command := "stopserver\r\n"
	io.Copy(conn, bytes.NewBufferString(command))
	log.Printf("Command for stopping server %d sent", serverConfig.Id)
	return true
}

func getLeaderConfig(t *testing.T) (raft.ServerConfig, error) {
	var leaderConfig raft.ServerConfig
	var err error
	verifyCommand := "getm saurabh\r\n"
	attempts := 0
	for {

		for _, server := range clusterConfig.Servers {
			version := executeGetMForVersion(t, verifyCommand, server)
			if version != -2 {
				leaderConfig = server
				break
			}
		}
		attempts++
		if leaderConfig.Hostname != "" {
			break
		} else if attempts < 3 {
			timer := time.NewTimer(1 * time.Second)
			<-timer.C
		} else {
			err = errors.New("Leader not found after 3 attempts")
		}
	}
	return leaderConfig, err
}
