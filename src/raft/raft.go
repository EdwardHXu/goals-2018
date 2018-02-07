package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
	// "log"
	// "reflect"
)

const Follower = 0
const Candidate = 1
const Leader = 2
const Dead = 3

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	CurrentTerm   int
	VoteCount     int
	VotedFor      int
	LogList       []Log
	CommitIndex   int
	LastApplied   int
	NextIndexList []int
	MatchIndex    []int
	State         int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.CurrentTerm, rf.State == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	lastLogIndex := -1
	lastLogTerm := -1
	/*
		if len(rf.LogList) > 0 {
			lastLogIndex = len(rf.LogList) - 1
			lastLogTerm = rf.LogList[lastLogIndex].Term
		}*/

	/*
		Figure 2 - RequestVote RPC:
		Reply false if term < currentTerm
	*/
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	/*
		Figure 2 - RequestVote RPC:
		If votedFor is null or candidateId, and candidate's log is at least
		as up-to-date as receiver's log, grant vote.
	*/
	fmt.Printf("[%d] rf.VotedFor: %d, args.CandidateId: %d, args.LastLogIndex: %d, lastLogIndex: %d, args.LastLogTerm: %d, lastLogTerm: %d\n",
		rf.me, rf.VotedFor, args.CandidateId, args.LastLogIndex, lastLogIndex, args.LastLogTerm, lastLogTerm)
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= lastLogTerm {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		fmt.Printf("[%d] reply.VoteGranted to %+v &reply: %p\n", rf.me, reply, &(*reply))
	}

	/*
		Figure 2 - Rules for Servers:
		If RPC request or response contains term T > currentTerm:
		set currentTerm = T, convert to follower
	*/
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = Follower
	}

	// fmt.Printf("[%d] RequestVote, %+v, %p\n", rf.me, reply,  &(*reply))
	return
}

type Log struct {
	Cmd  string
	Term int
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

/*
	Figure 2 - AppendEntries RPC
	Receiver implementation:
	1. Reply false if term < currentTerm
	2. Reply false if log doesn't contain an entry a prevLogIndex whose term matches prevLogTerm
	3. If an existing entry conflicts with a new one (same index but different terms), delete the
	   existing entry and all the follow it.
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

*/
func (rf *Raft) AppendEntry(args *AppendEntries, reply *AppendEntriesReply) {
	/*
		- "If the leader's term is at least as large as the candidate's current term,
		  then the candidate recognizes as legitimate and returns to follower state."
		  (page 6)
		- "If the term in the RPC is smaller than the candidate's current term, then
		   the candidate rejects the RPC and continues in candidate state."
		   (page 6)
		- args.Terms - Leader's current term
	*/
	// fmt.Printf("[%d] appending %d's entry\n", rf.me, args.LeaderId)
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else {
		if rf.State == Candidate {
			rf.State = Follower
			fmt.Printf("[%d] received append from %d\n", rf.me, args.LeaderId)
		}
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		reply.Success = true
	}

	/*if rf.LogList[args.PrevLogIndex] == Log{} {
		reply.Term    = rf.CurrentTerm
		reply.Success = false
	}*/

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("[%d] sendRequestVote: %+v\n", rf.me, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

/*
	Initiate an election after timeout
*/
func (rf *Raft) InitElection() bool {
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.VoteCount++
	fmt.Printf("[%d] candidate with %d votes\n", rf.me, rf.VoteCount)
	var wg sync.WaitGroup

	i := 0
	for i < len(rf.peers) {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				lastLogIndex := -1
				lastLogTerm := -1

				if len(rf.LogList) > 0 {
					lastLogIndex = len(rf.LogList) - 1
					lastLogTerm = rf.LogList[lastLogIndex].Term
				}

				args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, lastLogTerm}
				reply := RequestVoteReply{rf.CurrentTerm, false}

				if rf.sendRequestVote(i, &args, &reply) {
					// the other server received the vote request
					if reply.VoteGranted {
						rf.mu.Lock()
						fmt.Printf("[%d] reply.VotedGranted\n", rf.me)
						rf.VoteCount++

						if rf.VoteCount >= int(math.Ceil(float64(len(rf.peers)/2))) {
							rf.State = Leader
							fmt.Printf("[%d] is leader\n", rf.me)
						}
						rf.mu.Unlock()
					}
					fmt.Printf("[%d] state after send: %d req\n", rf.me, rf.State)
				}
				fmt.Printf("[%d] sent vote req to %d\n", rf.me, i)
			}(i)
		}
		i++
	}

	wg.Wait()
	return true
}

/*
	Leader should continuously ping peers until it receives replies from all....I think.
	Need reread paper to verify this.
*/
func (rf *Raft) PulseHeartbeat() {
	// fmt.Printf("[%d] is sending heartbeats\n", rf.me)
	i := 0
	for i < len(rf.peers) {
		if i != rf.me && rf.State != Dead {
			go func(i int) {

				// Construct heartbeat, which contains an empty list of logs ([]Log{})
				var prevLogIndex int
				var prevLogTerm int
				if len(rf.LogList) > 0 {
					prevLogIndex = len(rf.LogList) - 1
					prevLogTerm = rf.LogList[prevLogIndex].Term
				} else {
					prevLogIndex = 0
					prevLogTerm = 0
				}

				entry := AppendEntries{rf.CurrentTerm, rf.me, prevLogIndex, prevLogTerm, []Log{}, rf.CommitIndex}
				reply := AppendEntriesReply{}
				if rf.sendAppendEntry(i, &entry, &reply) {
					// fmt.Printf("[%d] got reply from %d", rf.me, i)
				}
			}(i)
		}
		i++
	}
}

/*
	Set the state of the server
*/
func (rf *Raft) SetState() {
	for {
		/*
			Figure 2 - All servers:
			- If commitIndex > lastApplied:
				increment lastApplied, apply log[lastApplied] to state machine
			- If RPC request or response contains term T > currentTerm: set
				set currentTerm = T, convert to follower
		*/
		/*if rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			//rf.LogList[rf.LastApplied] // Apply to state machine
		}
		*/
		/*
			Figure 2 - Followers:
			- Respond to RPCs from candidates and leaders
			- If election timeout elapses without receiving AppendEntries
				RPC from current leader or granting vote to candidate:
					convert to candiate
		*/

		if rf.State == Follower {
			rf.VoteCount = 0
			rf.VotedFor = -1
			transition := make(chan bool)

			// Set randomized timer and then start an election
			// time.AfterFunc(time.Duration(random(150, 350)) * time.Millisecond, func() {
			time.AfterFunc(time.Duration(280+(10*rf.me))*time.Millisecond, func() {
				// Vote for itself and change state to Candidate
				if rf.VotedFor == -1 {
					rf.State = Candidate
					transition <- true
				}
			})

			/*
				- Respond to RPC's from candidates and leaders
				- Need some mechanism to respond to requests or maybe there's some template stuff above...
			*/

			<-transition
		}

		/*
			Figure 2 - Candidates:
			- On conversion to candidate, start election:
				- Increment currentTerm
				- Vote for self
				- Reset election timer
				- Send RequestVote RPCs to all other servers
			- If votes received from majority of servers: become leader
			- If AppenedEntries RPC received from new leader: convert to follower
			- If election timeout elapses: start new election
		*/

		if rf.State == Candidate {
			/*timer := time.AfterFunc(time.Duration(random(150, 300)) * time.Millisecond, func() {
				rf.InitElection()
				fmt.Printf("[%d] state: %d, timeout\n", rf.me, rf.State)
			})*/
			rf.InitElection()

			/*if ok {
				if !timer.Stop() {
					<-timer.C
				}
			}*/

			fmt.Printf("[%d] finished election, state: %d\n", rf.me, rf.State)
			break
		}

		/*
			Figure 2 - Leaders:
			- Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
				server; repeat during idle periods to prevent election timeouts
			- If command received from client: append entry to local log,
				respond after entry applied to state machine
			- If last log index >= nextIndex for a follower: send AppendEntries RPC
				with log entries starting at nextIndex
					- If successful: update nextIndex and matchIndex for follower
					- If AppendEntries fails because of log inconsistency:
						decrement nextIndex and retry
			- If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
				and log[N].term == currentTerm:
					set commitIndex = N
		*/

		if rf.State == Leader {
			rf.PulseHeartbeat()
			transition := make(chan bool)
			// fmt.Printf("[%d] is the leader with %d votes\n", rf.me, rf.VoteCount)
			// Heartbeat timeout
			time.AfterFunc(time.Duration(random(150, 300))*time.Millisecond, func() {
				// Allow the previous term to end by unblocking
				rf.PulseHeartbeat()
				transition <- false
			})

			<-transition
		}

		if rf.State == Dead {
			break
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Printf("[%d] killed\n", rf.me)
	rf.State = Dead
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.VoteCount = 0
	rf.VotedFor = -1
	rf.LogList = []Log{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndexList = []int{}
	rf.MatchIndex = []int{}
	rf.State = Follower
	rf.CurrentTerm = 0

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func(rf *Raft) {
		rf.SetState()
	}(rf)

	return rf
}
