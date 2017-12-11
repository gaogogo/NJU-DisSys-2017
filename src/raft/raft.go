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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//server state
const FOLLOWER, CANDIDATE, LEADER = 1, 2, 3

//Null value
const NULL = -1

//timer
const TIMER_HEARTBEAT = 100 * time.Millisecond

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

// log entry
type LogEntrie struct {
	term    int
	index   int
	command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currenTerm int
	votedFor   int
	log        []LogEntrie

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on all leaders
	nextIndex  []int
	matchIndex []int

	serverState int
	voteCount   int

	//channel for state change
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanCommit    chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currenTerm, rf.serverState == LEADER
}

//
func (rf *Raft) LastLogInfo() (int, int) {
	return rf.log[len(rf.log)-1].index, rf.log[len(rf.log)-1].term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currenTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currenTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	if args.Term < rf.currenTerm {
		reply.Term = rf.currenTerm
		return
	}

	if args.Term > rf.currenTerm {
		rf.currenTerm = args.Term
		rf.serverState = FOLLOWER
		rf.votedFor = NULL
	}

	reply.Term = rf.currenTerm

	lastLogIndex, lastLogTerm := rf.LastLogInfo()

	argsNew := true
	if args.LastLogIndex < lastLogIndex || args.LastLogTerm < lastLogTerm {
		argsNew = false
	}

	if (rf.votedFor == NULL || rf.votedFor == args.CandidateId) && argsNew {
		reply.VoteGranted = true

		rf.chanGrantVote <- true
		rf.votedFor = args.CandidateId
		rf.serverState = FOLLOWER
	}

	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {

		if rf.serverState != CANDIDATE || args.Term != rf.currenTerm {
			return ok
		}

		if reply.Term > rf.currenTerm {
			rf.currenTerm = reply.Term
			rf.serverState = FOLLOWER
			rf.votedFor = NULL
			rf.persist()
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.serverState == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.serverState = LEADER
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

//
func (rf *Raft) brodcastRequestVote() {

	rf.mu.Lock()

	rf.currenTerm++
	rf.voteCount = 1
	rf.votedFor = rf.me

	rf.persist()

	var args RequestVoteArgs
	args.Term = rf.currenTerm
	args.CandidateId = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.LastLogInfo()

	rf.mu.Unlock()

	for i := range rf.peers {
		if rf.serverState == CANDIDATE && i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				rf.sendRequestVote(server, args, &reply)
			}(i)
		}
	}
}

//
type AppendEntriesArgs struct {
	//
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntrie
	LeaderCommit int
}

//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Printf("AppendEntries args.Term : %d \n", args.Term)
	reply.Success = false
	if args.Term < rf.currenTerm {
		reply.Term = rf.currenTerm
		return
	}

	rf.chanHeartbeat <- true
	if args.Term > rf.currenTerm {
		rf.currenTerm = args.Term
		rf.serverState = FOLLOWER
		rf.votedFor = NULL
	}

	reply.Term = args.Term
	last, _ := rf.LastLogInfo()

	if args.PrevLogIndex > last {
		return
	}

	base := rf.log[0].index
	if args.PrevLogIndex >= base {

		if args.PrevLogTerm != rf.log[args.PrevLogIndex-base].term {
			return
		}

		rf.log = rf.log[:args.PrevLogIndex+1-base]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
	}

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex, _ := rf.LastLogInfo()
		if args.LeaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}

	return
}

//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	rf.mu.Unlock()
	if ok {
		if rf.serverState != LEADER || args.Term != rf.currenTerm {
			return ok
		}
		if reply.Term > rf.currenTerm {
			rf.currenTerm = reply.Term
			rf.serverState = FOLLOWER
			rf.votedFor = NULL
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
	}
	return ok
}

//just send heartbeat, it will be modified in lab3
func (rf *Raft) brodcastAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, _ := rf.LastLogInfo()
	base := rf.log[0].index

	N := rf.commitIndex
	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {

		if rf.log[i-base].term != rf.currenTerm {
			continue
		}

		count := 1
		for s := range rf.peers {
			if s != rf.me && rf.matchIndex[s] >= i {
				count++
			}
		}
		if 2*count > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if rf.serverState == LEADER && i != rf.me && rf.nextIndex[i] > base {

			var args AppendEntriesArgs
			args.LeaderId = rf.me
			args.Term = rf.currenTerm

			args.PrevLogIndex = rf.nextIndex[i] - 1
			//fmt.Printf("args.PrevLogIndex : %d base : %d i:%d\n ", args.PrevLogIndex, base, i)
			args.PrevLogTerm = rf.log[args.PrevLogIndex-base].term
			args.Entries = make([]LogEntrie, len(rf.log[args.PrevLogIndex+1-base:]))
			copy(args.Entries, rf.log[args.PrevLogIndex+1-base:])
			args.LeaderCommit = rf.commitIndex

			go func(server int) {
				var reply AppendEntriesReply
				//fmt.Printf("args.Term : %d\n", args.Term)
				rf.sendAppendEntries(server, args, &reply)
			}(i)
		}
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := NULL
	term := rf.currenTerm
	isLeader := rf.serverState == LEADER

	if isLeader {
		lastLogIndex, _ := rf.LastLogInfo()
		index = lastLogIndex + 1
		rf.log = append(rf.log, LogEntrie{term: term, index: index, command: command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
func (rf *Raft) followerHandle() {
	select {
	case <-rf.chanHeartbeat:
	case <-rf.chanGrantVote:
	case <-time.After(time.Duration(rand.Int63()%150+200) * time.Millisecond):
		rf.serverState = CANDIDATE
	}
}

//
func (rf *Raft) candidateHandle() {
	go rf.brodcastRequestVote()
	select {
	case <-rf.chanHeartbeat:
		rf.serverState = FOLLOWER
	case <-rf.chanLeader:
		rf.mu.Lock()
		rf.serverState = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastLogIndex, _ := rf.LastLogInfo()

		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			//fmt.Printf("rf.nextIndex[%d] : %d\n", i, rf.nextIndex[i])
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
	case <-time.After(time.Duration(rand.Int63()%150+200) * time.Millisecond):
	}
}

//
func (rf *Raft) leaderHandle() {
	rf.brodcastAppendEntries()
	time.Sleep(TIMER_HEARTBEAT)
}

//
func (rf *Raft) RaftHandle() {
	for {
		switch rf.serverState {

		case FOLLOWER:
			rf.followerHandle()
		case CANDIDATE:
			rf.candidateHandle()
		case LEADER:
			rf.leaderHandle()
		}
	}
}

//
func (rf *Raft) AppliedMsg(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.chanCommit:
			rf.mu.Lock()
			base := rf.log[0].index
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.log[i-base].command}
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
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

	// Your initialization code here.
	rf.serverState = FOLLOWER
	rf.currenTerm = 0
	rf.votedFor = NULL
	rf.log = append(rf.log, LogEntrie{term: 0})

	rf.chanGrantVote = make(chan bool, 50)
	rf.chanHeartbeat = make(chan bool, 50)
	rf.chanLeader = make(chan bool, 50)
	rf.chanCommit = make(chan bool, 50)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.RaftHandle()
	go rf.AppliedMsg(applyCh)

	return rf
}
