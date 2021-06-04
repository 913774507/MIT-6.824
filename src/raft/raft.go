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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const electionDuration int64 = 1000

const (
	FOLLWER int = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int // 0:follwer 1:candiate 2:leader
	currentTerm int
	votedFor    int // peerId
	log         []LogEntry

	commitIndex int // last commited log index
	lastApplied int // last applied log index

	nextIndex  []int // only for leader
	matchIndex []int // only for leader

	applyCh        chan ApplyMsg
	heartbeatCh    chan *AppendEntriesArgs
	follwerCh      chan interface{}
	timerTime      time.Duration
	timer          *time.Timer
	heartBeatTimer *time.Timer
}

func getRandomTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randTime := rand.Int63() % 1000
	return time.Duration((randTime + electionDuration) * int64(time.Millisecond))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.dead == 1 {
		return 0, false
	}
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Err         bool
	Server      int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("S%v get request vote args term:%v, candidateId:%v, lastlogindex:%v, lastlogterm:%v  currentterm:%v\n", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		fmt.Printf("S%v reject S%v\n", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm { //reconnect 检查日志才能ac
		if rf.state == FOLLWER {
			if rf.votedFor == -1 {
				rf.currentTerm = args.Term
				reply.Term, reply.VoteGranted = rf.currentTerm, true
				rf.votedFor = args.CandidateId
				rf.timer.Reset(getRandomTime())
				fmt.Printf("S%v votefor S%v\n", rf.me, args.CandidateId)
				return
			} else {
				reply.Term, reply.VoteGranted = rf.currentTerm, false
				fmt.Printf("S%v reject S%v, votedFor:%v\n", rf.me, args.CandidateId, rf.votedFor)
				return
			}
		} else if rf.state == LEADER {
			rf.state = FOLLWER //每次切换到follwer, votedFor置-1
			rf.currentTerm = args.Term
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = args.CandidateId
			rf.follwerCh <- 1
			rf.timer.Reset(getRandomTime())
			fmt.Printf("S%v from leader change to follwer, votefor S%v\n", rf.me, args.CandidateId)
			return
		} else if rf.state == CANDIDATE {
			rf.state = FOLLWER //每次切换到follwer, votedFor置-1
			rf.currentTerm = args.Term
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = args.CandidateId
			rf.timer.Reset(getRandomTime())
			fmt.Printf("S%v from candidate change to follwer, votefor S%v\n", rf.me, args.CandidateId)
			return
		}
	}
	rfLastLogIndex := 0
	rfLastLogTerm := 0
	fmt.Printf("S%v rflastlogindex:%v, rflastterm:%v\n", rf.me, rfLastLogIndex, rfLastLogTerm)
	if len(rf.log) > 0 {
		rfLastLogIndex = len(rf.log) - 1
		rfLastLogTerm = rf.log[rfLastLogIndex-1].Term
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogIndex >= rfLastLogIndex && args.LastLogTerm >= rfLastLogTerm {
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = args.CandidateId
			fmt.Printf("S%v votefor S%v, 257\n", rf.me, args.CandidateId)
			return
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	fmt.Printf("S%v reject S%v\n", rf.me, args.CandidateId)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyCh chan *RequestVoteReply) {
	reply := &RequestVoteReply{
		
	}
	reply.Server = server
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		reply.Err = true
	}
	replyCh <- reply
	fmt.Printf("S%v reply S%v to ch\n", server, args.CandidateId)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        //上一个log index
	PrevLogTerm  int        //
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.heartbeatCh <- args
	// fmt.Printf("S%v get heatbeat from S%v\n", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (len(rf.log) > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		if rf.state == CANDIDATE {
			rf.state = FOLLWER
			rf.votedFor = -1
			fmt.Printf("344 S%v from candidate change to follower, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
			rf.timer.Reset(getRandomTime())
		} else if rf.state == LEADER {
			rf.state = FOLLWER
			rf.votedFor = -1
			rf.follwerCh <- 1
			fmt.Printf("350 S%v change to follower, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
			rf.timer.Reset(getRandomTime())
		} else {
			rf.votedFor = -1
			fmt.Printf("354 S%v get heartbeat, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
			rf.timer.Reset(getRandomTime())
		}
		rf.currentTerm = args.Term
		reply.Success = true
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == FOLLWER {
			rf.votedFor = -1
			rf.timer.Reset(getRandomTime())
			fmt.Printf("353 S%v argsTerm==curTerm, ac, from S%v\n", rf.me, args.LeaderId)
			reply.Success = true
		} else if rf.state == CANDIDATE {
			fmt.Printf("353 S%v argsTerm==curTerm, ac, from candidate change to follower, from S%v\n", rf.me, args.LeaderId)
			rf.state = FOLLWER
			rf.votedFor = -1
			rf.timer.Reset(getRandomTime())
			reply.Success = true
		} else if rf.state == FOLLWER {
			fmt.Printf("353 S%v argsTerm==curTerm, reject, state:%v, from S%v\n", rf.me, rf.state, args.LeaderId)
			reply.Success = false
		}
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the lead;.er.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timer.C:
		ELECTION:
			fmt.Printf("S%v start election\n", rf.me)
			//trans to candidate
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm += 1
			rf.votedFor = rf.me
			nowVotes := 1
			lastLogIndex := 0
			lastLogTerm := 0
			if len(rf.log) > 0 {
				lastLogIndex = len(rf.log)
				lastLogTerm = rf.log[lastLogIndex].Term
			}
			rf.mu.Unlock()
			replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			for i := range rf.peers {
				if i != rf.me {
					fmt.Printf("S%v send request to S%v\n", rf.me, i)
					go rf.sendRequestVote(i, args, replyCh)
				}
			}
			rf.timer.Reset(getRandomTime())
			for {
				select {
				case r := <-replyCh:
					fmt.Printf("S%v get reply from S%v, voteGranted:%v\n", rf.me, r.Server, r.VoteGranted)
					if r.Err {
						// go rf.sendRequestVote(r.Server, args, replyCh)
					} else if r.VoteGranted {
						nowVotes += 1
						fmt.Printf("463 S%v nowVotes:%v, threshold:%v\n", rf.me, nowVotes, len(rf.peers))
						if nowVotes*2 > len(rf.peers) {
							fmt.Printf("S%v become leader\n", rf.me)
							//change to leader
							rf.state = LEADER
							rf.timer.Stop()
							// send heartbeat
							go rf.heartBeat()
							return
						}
					} else if r.Term > rf.currentTerm {
						// step down
						if rf.state == CANDIDATE {
							rf.state = FOLLWER
							rf.votedFor = -1
							rf.currentTerm = r.Term
							rf.timer.Reset(getRandomTime())
						}
					}
				case <-rf.timer.C:
					fmt.Printf("S%v election timeout\n", rf.me)
					rf.timer.Reset(getRandomTime())
					goto ELECTION
				}
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.heartbeat()
	<-rf.heartBeatTimer.C
	rf.heartBeatTimer.Reset(time.Duration(150 * time.Millisecond))
	for rf.killed() == false {
		select {
		case <-rf.heartBeatTimer.C:
			rf.heartbeat()
			rf.heartBeatTimer.Reset(time.Duration(150 * time.Millisecond))
		case <-rf.follwerCh:
			go rf.ticker()
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	prevLogIndex := 0
	prevLogTerm := 0
	if len(rf.log) > 0 {
		prevLogIndex = len(rf.log) - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	for i := range rf.peers {
		if i != rf.me {
			AEReply := &AppendEntriesReply{}
			go rf.sendAppendEntries(i, args, AEReply)
			// fmt.Printf("S%v send AE to S%v\n", rf.me, i)
		}
	}
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
	rf.votedFor = -1
	rf.state = FOLLWER
	rf.dead = -1
	rf.log = make([]LogEntry, 0)
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.follwerCh = make(chan interface{}, 5)
	rf.timer = time.NewTimer(getRandomTime())
	rf.heartBeatTimer = time.NewTimer(time.Duration(150 * time.Millisecond))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
