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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const electionDuration int64 = 1000
const heartBeatDuration time.Duration = 100 * time.Millisecond

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
	preVotedFor int
	log         []LogEntry

	commitIndex int // last commited log index
	lastApplied int // last applied log index

	nextIndex  []int // (only for leader) for each server, index of the next log entry to send to that server(initialized to leaderlast log index + 1)
	matchIndex []int // (only for leader) for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.dead == 1 {
		return 0, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
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
	PreVoteFlag  bool
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
	DPrintf("S%v get request vote args term:%v, candidateId:%v, lastlogindex:%v, lastlogterm:%v  currentterm:%v\n", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("S%v reject S%v\n", rf.me, args.CandidateId)
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("S%v reject S%v, has voted for S%v\n", rf.me, args.CandidateId, rf.votedFor)
		return
	}

	rfLastLogIndex := 0
	rfLastLogTerm := 0
	// DPrintf("S%v rflastlogindex:%v, rflastterm:%v\n", rf.me, rfLastLogIndex, rfLastLogTerm)
	if len(rf.log) > 0 {
		rfLastLogIndex = len(rf.log)
		rfLastLogTerm = rf.log[rfLastLogIndex-1].Term
	}
	if rfLastLogTerm > args.LastLogTerm || // the server has log with higher term
		(rfLastLogIndex == args.LastLogTerm && rfLastLogIndex > args.LastLogIndex) { // under same term, this server has longer index
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("S%v reject S%v, log not new\n", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm { //reconnect 检查日志才能ac
		if rf.state == FOLLWER {
			if rf.votedFor == -1 {
				if args.PreVoteFlag {
					rf.preVotedFor = args.CandidateId
					reply.Term, reply.VoteGranted = rf.currentTerm, true
					rf.timer.Reset(getRandomTime())
					DPrintf("S%v preVotefor S%v\n", rf.me, args.CandidateId)
					return
				}
				rf.currentTerm = args.Term
				reply.Term, reply.VoteGranted = rf.currentTerm, true
				rf.votedFor = args.CandidateId
				rf.timer.Reset(getRandomTime())
				DPrintf("S%v votefor S%v\n", rf.me, args.CandidateId)
				return
			} else {
				reply.Term, reply.VoteGranted = rf.currentTerm, false
				DPrintf("S%v reject S%v, votedFor:%v\n", rf.me, args.CandidateId, rf.votedFor)
				return
			}
		} else if rf.state == LEADER {
			if args.PreVoteFlag {
				rf.preVotedFor = args.CandidateId
				reply.Term, reply.VoteGranted = rf.currentTerm, true
				rf.timer.Reset(getRandomTime())
				DPrintf("S%v preVotefor S%v\n", rf.me, args.CandidateId)
				return
			}
			rf.state = FOLLWER
			rf.currentTerm = args.Term
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = args.CandidateId
			rf.follwerCh <- 1
			rf.timer.Reset(getRandomTime())
			DPrintf("S%v from leader change to follwer, votefor S%v\n", rf.me, args.CandidateId)
			return
		} else if rf.state == CANDIDATE {
			if args.PreVoteFlag {
				rf.preVotedFor = args.CandidateId
				reply.Term, reply.VoteGranted = rf.currentTerm, true
				rf.timer.Reset(getRandomTime())
				DPrintf("S%v preVotefor S%v\n", rf.me, args.CandidateId)
				return
			}
			rf.state = FOLLWER
			rf.currentTerm = args.Term
			reply.Term, reply.VoteGranted = rf.currentTerm, true
			rf.votedFor = args.CandidateId
			rf.timer.Reset(getRandomTime())
			DPrintf("S%v from candidate change to follwer, votefor S%v\n", rf.me, args.CandidateId)
			return
		}
	}

	if args.PreVoteFlag {
		rf.preVotedFor = args.CandidateId
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		rf.timer.Reset(getRandomTime())
		DPrintf("S%v preVotefor S%v\n", rf.me, args.CandidateId)
		return
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.votedFor = args.CandidateId
	rf.timer.Reset(getRandomTime())
	DPrintf("S%v vote S%v\n", rf.me, args.CandidateId)
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
	reply := &RequestVoteReply{}
	reply.Server = server
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		reply.Err = true
	}
	replyCh <- reply
	DPrintf("S%v reply S%v to ch\n", server, args.CandidateId)
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
	DPrintf("S%v get heatbeat from S%v\n", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("S%v argsTerm<curTerm, reject, state:%v, from S%v\n", rf.me, rf.state, args.LeaderId)
		reply.Success = false
		return
	}
	if args.PrevLogIndex > 0 && len(rf.log) > args.PrevLogIndex {
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			DPrintf("S%v argsPrevLogTerm:%v rfPrevLogTerm:%v, reject from S%v\n", rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex-1].Term, args.LeaderId)
			reply.Success = false
			return
		}
	}
	if args.Term > rf.currentTerm {
		if rf.state == CANDIDATE {
			rf.state = FOLLWER
			rf.votedFor = -1
			DPrintf("S%v from candidate change to follower, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		} else if rf.state == LEADER {
			rf.state = FOLLWER
			rf.votedFor = -1
			rf.follwerCh <- 1
			DPrintf("S%v change to follower, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		} else {
			rf.votedFor = -1
			DPrintf("S%v get heartbeat, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		rf.currentTerm = args.Term
		reply.Success = true
		rf.timer.Reset(getRandomTime())
	} else {
		if rf.state == FOLLWER || rf.state == CANDIDATE {
			rf.state = FOLLWER
			rf.votedFor = -1
			rf.timer.Reset(getRandomTime())
			reply.Success = true
			DPrintf("S%v argsTerm==curTerm, ac, from S%v\n", rf.me, args.LeaderId)
		} else if rf.state == LEADER {
			DPrintf("S%v argsTerm==curTerm, reject, state:%v, from S%v\n", rf.me, rf.state, args.LeaderId)
			reply.Success = false
			// rf.state = FOLLWER
			// rf.votedFor = -1
			// rf.follwerCh <- 1
			// DPrintf("376 S%v change to follower, argsTerm:%v, curTerm:%v, from S%v\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
	}
	if rf.state == FOLLWER {
		if len(args.Entries) > 0 {
			rf.log = rf.log[:args.PrevLogIndex]
			rf.log = append(rf.log, args.Entries...)
			DPrintf("S%v appendlog:%v form S%v, nowlog:%v, leaderCommit:%v", rf.me, args.Entries, args.LeaderId, rf.log, args.LeaderCommit)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DPrintf("S%v update commitIdx %v", rf.me, rf.commitIndex)
		rf.updateLastApplied()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		curLog := rf.log[rf.lastApplied-1]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      curLog.Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("S%v apply msg %v", rf.me, msg)
		rf.applyCh <- msg
	}
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	//日志写入本机log
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	nowLogIndex := len(rf.log)
	DPrintf("S%v leader append log, nowLogIndex:%v, nowlog:%v", rf.me, nowLogIndex, rf.log)
	rf.mu.Unlock()

	//复制到follower
	var sum int32 = 1
	for i := range rf.peers {
		if i != rf.me {
			// DPrintf("S%v replic logindex:%v to S%v\n", rf.me, nowLogIndex, i)
			go func(i int) {
				reply := &AppendEntriesReply{}
				for {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.getPrevLogIndex(i),
						PrevLogTerm:  rf.getPrevLogTerm(i),
						Entries:      rf.log[rf.nextIndex[i]-1:],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					DPrintf("S%v replic log:%v to S%v\n", rf.me, args.Entries, i)
					ok := rf.sendAppendEntries(i, args, reply)
					if !ok {
						DPrintf("S%v disconnect to S%v\n", rf.me, i)
						time.Sleep(100 * time.Millisecond)
					} else if reply.Success {
						DPrintf("S%v replic to S%v success\n", rf.me, i)
						rf.mu.Lock()
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						atomic.AddInt32(&sum, 1)
						if int(sum*2) == len(rf.peers)+1 {
							rf.commitIndex = Max(nowLogIndex, rf.commitIndex)
							DPrintf("S%v leader update commitIndex:%v", rf.me, rf.commitIndex)
							rf.updateLastApplied()
						}
						rf.mu.Unlock()
						return
					} else if reply.Term > rf.currentTerm {
						DPrintf("S%v find higher term:%v from S%v\n", rf.me, reply.Term, i)
						rf.mu.Lock()
						rf.state = FOLLWER
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.follwerCh <- 1
						rf.timer.Reset(getRandomTime())
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Lock()
						if rf.nextIndex[i] > 1 {
							DPrintf("S%v decrease nextIndex[%v]:%v\n", rf.me, i, rf.nextIndex[i])
							rf.nextIndex[i] -= 1
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}

	rf.mu.Lock()
	index = nowLogIndex
	term = rf.currentTerm
	isLeader = true
	rf.mu.Unlock()
	
	return index, term, isLeader
}

func (rf *Raft) getPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIndex := rf.getPrevLogIndex(i)
	if prevLogIndex == 0 {
		return 0
	}
	return rf.log[prevLogIndex-1].Term
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

func (rf *Raft) preVote() bool {
	DPrintf("S%v start preVote\n", rf.me)
	var nowVotes int32 = 1
	lastLogIndex := 0
	lastLogTerm := 0

	rf.mu.Lock()
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVoteFlag:  true,
	}
	rf.mu.Unlock()

	vTimer := time.NewTimer(getRandomTime())
	for i := range rf.peers {
		if i != rf.me {
			DPrintf("S%v send preRequest to S%v\n", rf.me, i)
			go rf.sendRequestVote(i, args, replyCh)
		}
	}

	for {
		select {
		case r := <-replyCh:
			DPrintf("S%v get preReply from S%v, voteGranted:%v\n", rf.me, r.Server, r.VoteGranted)
			if r.VoteGranted {
				atomic.AddInt32(&nowVotes, 1)
				DPrintf("S%v nowPreVotes:%v, threshold:%v\n", rf.me, nowVotes, len(rf.peers))
				if int(nowVotes*2) > len(rf.peers) {
					DPrintf("S%v preVote success\n", rf.me)
					return true
				}
			}
		case <-vTimer.C:
			DPrintf("S%v preVote timeout\n", rf.me)
			return false
		}
	}
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
			if !rf.preVote() {
				rf.timer.Reset(getRandomTime())
				continue
			}
			DPrintf("S%v start election\n", rf.me)
			//trans to candidate
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm += 1
			rf.votedFor = rf.me
			var nowVotes int32 = 1
			lastLogIndex := 0
			lastLogTerm := 0
			if len(rf.log) > 0 {
				lastLogIndex = len(rf.log)
				lastLogTerm = rf.log[lastLogIndex-1].Term
			}
			replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			rf.mu.Unlock()

			for i := range rf.peers {
				if i != rf.me {
					DPrintf("S%v send request to S%v\n", rf.me, i)
					go rf.sendRequestVote(i, args, replyCh)
				}
			}
			rf.timer.Reset(getRandomTime())
			for {
				select {
				case r := <-replyCh:
					DPrintf("S%v get reply from S%v, voteGranted:%v\n", rf.me, r.Server, r.VoteGranted)
					if r.Err {
						// go rf.sendRequestVote(r.Server, args, replyCh)
					} else if r.VoteGranted {
						atomic.AddInt32(&nowVotes, 1)
						DPrintf("463 S%v nowVotes:%v, threshold:%v\n", rf.me, nowVotes, len(rf.peers))
						if int(nowVotes*2) > len(rf.peers) {
							DPrintf("S%v become leader\n", rf.me)
							//change to leader
							rf.mu.Lock()
							rf.state = LEADER
							rf.nextIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								rf.nextIndex[i] = len(rf.log) + 1
							}
							rf.matchIndex = make([]int, len(rf.peers))
							rf.timer.Stop()
							rf.mu.Unlock()
							// send heartbeat
							go rf.heartBeat()
							return
						}
					} else if r.Term > rf.currentTerm {
						// step down
						rf.mu.Lock()
						if rf.state == CANDIDATE {
							rf.state = FOLLWER
							rf.votedFor = -1
							rf.currentTerm = r.Term
							rf.timer.Reset(getRandomTime())
						}
						rf.mu.Unlock()
					}
				case <-rf.timer.C:
					DPrintf("S%v election timeout\n", rf.me)
					rf.timer.Reset(getRandomTime())
					goto ELECTION
				}
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.heartbeat()
	rf.heartBeatTimer.Reset(heartBeatDuration)
	// for rf.killed() == false && rf.state == LEADER {
	for rf.killed() == false {
		select {
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.heartbeat()
			rf.heartBeatTimer.Reset(heartBeatDuration)
		case <-rf.follwerCh:
			DPrintf("S%v change to follwer go ticker", rf.me)
			rf.timer.Reset(getRandomTime())
			rf.heartBeatTimer.Reset(heartBeatDuration)
			go rf.ticker()
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				DPrintf("S%v send AE to S%v\n", rf.me, i)
				AEReply := &AppendEntriesReply{}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(i),
					PrevLogTerm:  rf.getPrevLogTerm(i),
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				rf.sendAppendEntries(i, args, AEReply)

				rf.mu.Lock()
				if AEReply.Term > rf.currentTerm {
					rf.state = FOLLWER
					rf.votedFor = -1
					rf.currentTerm = AEReply.Term
					rf.follwerCh <- 1
					rf.timer.Reset(getRandomTime())
					DPrintf("S%v AE find higher term:%v", rf.me, AEReply.Term)
				}
				rf.mu.Unlock()

			}(i)
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
	rf.heartBeatTimer = time.NewTimer(heartBeatDuration)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
