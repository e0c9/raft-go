package raft

import (
	"math/rand"
	"raft-go/labrpc"
	"sync"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers

	// Raft server must maintain
	// Persistent state on all servers
	currentTerm int
	voteFor     int
	logs        []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// needed by communication
	status      int
	numOfVotes  int
	heartbeat   chan bool
	granted     chan bool
	winElection chan bool
	applyCh     chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.status == Leader
	return term, isleader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) persist() {
	// Example
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	// example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 {
		return
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
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

func (rf *Raft) isUpToDate(cIndex int, cTerm int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	if cTerm != term {
		return cTerm > term
	}
	return cIndex >= index
}

// 2B
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Raft[%v](Term[%v]) receives requestVoteRpc", rf.me, rf.currentTerm)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.voteFor = -1
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	/*
		If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote
	*/
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.granted <- true
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || args.Term != rf.currentTerm || rf.status != Candidate {
		return ok
	}

	// RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.voteFor = -1
		return ok
	}

	if reply.VoteGranted {
		rf.numOfVotes++
		// receives votes from a majority of the servers
		if rf.numOfVotes > len(rf.peers)/2 {
			DPrintf("Candidate[%v](Term[%v]) wins an election, logindex[%v] Term[%v]", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
			rf.status = Leader
			rf.winElection <- true
		}
	}
	return ok
}

func (rf *Raft) sendAllRequestVotes() {
	rf.mu.Lock()
	//DPrintf("Candidate[%v](Term[%v]) sends requestVote Rpc to each other servers ", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	// for i := 0; i < len(rf.peers) && i != rf.me && rf.status == Candidate; i++ {
	// 	go rf.sendRequestVote(i, args, &RequestVoteReply{})
	// }
	for i := range rf.peers {
		if i != rf.me && rf.status == Candidate {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Peer[%v](Term [%v]) receives (Leader %v)'s heartbeat", rf.me, rf.currentTerm, args.LeaderId)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.voteFor = -1
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	/* log doesn't contain an entry at prevlogindex whose term matches prevlogterm.
	. follower does not find an entry in its log with the same index and term, refuses
	. the new entries.
	*/
	if args.PrevLogIndex > rf.getLastLogIndex() ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	restLogs := rf.logs[args.PrevLogIndex+1:]
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	if rf.hasConflict(restLogs, args.Entries) {
		rf.logs = append(rf.logs, args.Entries...)
	} else {
		rf.logs = append(rf.logs, restLogs...)
	}

	/* leaderCommit > commitIndex, set commitIndex =
	min(leaderCommit, index of last new entry)
	*/
	//idx := args.PrevLogIndex + len(args.Entries)
	idx := rf.getLastLogIndex()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > idx {
			rf.commitIndex = idx
		}
		go rf.applyLogs()
	}

	reply.Success = true

	return
}

func (rf *Raft) hasConflict(logs []Log, entries []Log) bool {
	for i := range entries {
		if i > len(logs)-1 || entries[i].Term != logs[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.status != Leader || args.Term != rf.currentTerm {
		return ok
	}

	// RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.voteFor = -1
		return ok
	}

	// after rejection, the leader decrements nextIndex
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = args.PrevLogIndex
	}

	//  N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		count := 0

		if rf.logs[N].Term == rf.currentTerm {
			for i := range rf.matchIndex {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLogs()
			break
		}
	}

	return ok
}

func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Leader[%v](Term [%v]) sends heartbeat to each other servers", rf.me, rf.currentTerm)

	// for i := 0; i < len(rf.peers) && i != rf.me && rf.status == Leader; i++ {
	// 	go rf.sendAppendEntries(i, appArgs, &AppendEntriesReply{})
	// }
	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			appArgs := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
			}
			if rf.nextIndex[i] <= rf.getLastLogIndex() {
				// send log entries
				appArgs.Entries = rf.logs[rf.nextIndex[i]:]
			}

			go rf.sendAppendEntries(i, appArgs, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Command: rf.logs[i].Command, Index: i}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Leader[%v] received a client request", rf.me)
	term, isLeader := rf.currentTerm, rf.status == Leader
	index := rf.getLastLogIndex()

	if isLeader {
		// command received from client: append entry to local log
		rf.logs = append(rf.logs, Log{Command: command, Term: term})
		index = rf.getLastLogIndex()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func randElectionTimeout() int {
	return rand.Intn(150) + 200
}

func (rf *Raft) runServer() {
	for {
		switch rf.status {
		case Leader:
			// send heartbeat to each server
			rf.sendAllAppendEntries()
			time.Sleep(100 * time.Millisecond)
		case Follower:
			select {
			/*
				receive AppendEntries Rpc from Leader
				grant vote to Candidate
			*/
			case <-rf.granted:
			case <-rf.heartbeat:
			// election timeout elapses convert to Candidate
			case <-time.After(time.Duration(randElectionTimeout()) * time.Millisecond):
				rf.status = Candidate
			}
		case Candidate:
			/**
			increment currentTerm
			vote for self
			reset election timer
			send requestVoteRpc to all other servers
			*/
			rf.mu.Lock()
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.numOfVotes = 1
			rf.mu.Unlock()
			rf.sendAllRequestVotes()

			select {
			// election timeout elapses: start new election
			case <-time.After(time.Duration(randElectionTimeout()) * time.Millisecond):
			case <-rf.heartbeat: // receive AppendEntries Rpc from leader: convert to follower
				rf.status = Follower
			case <-rf.winElection: // receive votes from majority of servers: become Leader
				rf.mu.Lock()
				rf.status = Leader
				// Reinitialized after election
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				idx := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = idx
				}

				rf.mu.Unlock()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteFor = -1
	rf.numOfVotes = 0
	rf.status = Follower
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, Log{Term: 0})

	rf.heartbeat = make(chan bool)
	rf.granted = make(chan bool)
	rf.winElection = make(chan bool)

	rf.readPersist(persister.ReadRaftState())

	go rf.runServer()

	return rf
}
