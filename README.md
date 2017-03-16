#CS733 - Engineering a cloud

Saurabh Hote 13305R008

# Description
This piece of work is an implementation of Raft protocol for distributed consensus. The reference paper can be found at https://ramcloud.stanford.edu/raft.pdf
In Raft, consistency is achieved by ensuring that if any server has applied a particular log entry to its state machine, then no other server may apply a different command for the same log index.
There are two main features of this implementation, namely -

<b>1. Leader Election: </b> <br>
<p>
	The leader is responsible for accepting the client request and replicating the log across server replicas. Every server replica on start-up contends for becoming the leader by changing its state to CANDIDATE upon expiry of heartbeat timers.
	Candidate getting majority of votes gets elected as a leader. Since there can be only one candidate receiving the majority for an election process (election term), a system will always elect only one leader. 
</p>

<b>2. Log Synchronization:</b> <br>
<p>
	The elected leader upon receiving command request from clients performs append operation on the local log structure and then replicates this entry to other servers. The leader proceeds onto committing the new log entry only when it receives positive ack from the majority of the server replicas. 
</p>
# Features
* The code is divided into three different modules namely the Connection_Handler, the SharedLog and the KVStore
* All Client communication is handled by the Leader. The leader needs to get consensus from the majority of server replicas before committing the commands and executing them onto the state machine
* If a client tries to communicate with a non-leader server then it gets "ERR_REDIRECT host port" message where host=ip-address/name of Leader and port=port# of leader
* This implementation uses boltdb (https://www.progville.com/go/bolt-embedded-db-golang/) for log on-disk management 
* golang's 'net' package is used for communication between the server replicas


# Instructions: How to run
1.Obtain a copy of the project using:	

	go get github.com/saurabh-hote/cs733
	
	go get github.com/boltdb/bolt
	
	go get github.com/codegangsta/cli

2.Change directory to assignment-3 folder :

	cd $GOPATH/src/github.com/saurabh-hote/cs733/assignment-3/

3.Run the test script using following commands: 

	go test

Thank you!
 
# miniraft
