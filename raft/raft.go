package raft

import (
	"bytes"
	"encoding/gob"
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
	Index   int
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
	votedFor    int
	logs        []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders , Reinitialized after election
	nextIndex  []int
	matchIndex []int

	// needed by communication
	status      int
	numOfVotes  int
	heartbeat   chan bool
	granted     chan bool
	winElection chan bool
	applyCh     chan ApplyMsg
	snapshot    []byte
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.status == Leader
	return term, isleader
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)

}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var lastIncludedIndex int
	var lastIncludedTerm int
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.logs = rf.truncateLog(lastIncludedIndex, lastIncludedTerm)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	go func() {
		rf.applyCh <- msg
	}()
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
	Term      int
	Success   bool
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.heartbeat <- true
	rf.status = Follower
	rf.persister.SaveSnapshot(args.Data)
	rf.logs = rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.applyCh <- msg
}

func (rf *Raft) truncateLog(lastIncludedIndex int, lastIncludedTerm int) []Log {
	var newLogs []Log
	newLogs = append(newLogs, Log{Index: lastIncludedIndex, Term: lastIncludedTerm})
	for index := len(rf.logs) - 1; index >= 0; index-- {
		if rf.logs[index].Index == lastIncludedIndex && rf.logs[index].Term == lastIncludedTerm {
			newLogs = append(newLogs, rf.logs[index+1:]...)
			break
		}
	}

	return newLogs
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
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
		rf.votedFor = -1
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	/*
		If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote
	*/
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.granted <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

	rf.persist()
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
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.numOfVotes++
		// receives votes from a majority of the servers
		if rf.numOfVotes > len(rf.peers)/2 {
			// DPrintf("Candidate[%v](Term[%v]) wins an election, logindex[%v] Term[%v]", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
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
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	/* log doesn't contain an entry at prevlogindex whose term matches prevlogterm.
	. follower does not find an entry in its log with the same index and term, refuses
	. the new entries.
	*/

	// optimize to reduce the number of rejected AppendEntries RPCs
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}
	base := rf.logs[0].Index
	//fmt.Println(strconv.Itoa(base) + " " + strconv.Itoa(args.PrevLogIndex))
	if base < args.PrevLogIndex {
		if rf.logs[args.PrevLogIndex-base].Term != args.PrevLogTerm {
			term := rf.logs[args.PrevLogIndex-base].Term
			idx := args.PrevLogIndex

			for ; idx >= base && rf.logs[idx-base].Term == term; idx-- {
			}
			reply.NextIndex = idx + 1
			return
		}
	}
	if base <= args.PrevLogIndex {
		restLogs := rf.logs[args.PrevLogIndex+1-base:]
		rf.logs = rf.logs[:args.PrevLogIndex+1-base]
		if rf.hasConflict(restLogs, args.Entries) {
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			rf.logs = append(rf.logs, restLogs...)
		}
	}

	rf.persist()
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
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	// after rejection, the leader decrements nextIndex
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.NextIndex
	}

	//  N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	base := rf.logs[0].Index

	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		count := 1 // except the leader
		//	fmt.Println(strconv.Itoa(N) + " " + strconv.Itoa(base) + " " + strconv.Itoa(rf.commitIndex))
		if rf.logs[N-base].Term == rf.currentTerm {
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
	base := rf.logs[0].Index
	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			if rf.nextIndex[i] > base {
				appArgs := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1-base].Term,
				}
				if rf.nextIndex[i] <= rf.getLastLogIndex() {
					// send log entries
					appArgs.Entries = rf.logs[rf.nextIndex[i]-base:]
				}

				go rf.sendAppendEntries(i, appArgs, &AppendEntriesReply{})
			} else {
				args := &InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LastIncludedIndex = rf.logs[0].Index
				args.LastIncludedTerm = rf.logs[0].Term
				args.Data = rf.persister.snapshot
				go rf.sendInstallSnapShot(i, args, &InstallSnapshotReply{})
			}
		}
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.logs[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Command: rf.logs[i-base].Command, Index: i}
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
		rf.logs = append(rf.logs, Log{Command: command, Term: term, Index: index + 1})
		rf.persist()
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
			rf.votedFor = rf.me
			rf.numOfVotes = 1
			rf.persist()
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
	rf.votedFor = -1
	rf.numOfVotes = 0
	rf.status = Follower
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, Log{Term: 0})

	rf.heartbeat = make(chan bool)
	rf.granted = make(chan bool)
	rf.winElection = make(chan bool)

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	//rf.persist()

	go rf.runServer()

	return rf
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

type SnapShot struct {
	Data      []byte
	LogOffset int
	Term      int
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.logs[0].Index
	lastIndex := rf.getLastLogIndex()
	if index <= baseIndex || index > lastIndex {
		return
	}
	var newLogs []Log
	newLogs = append(newLogs, Log{Term: rf.logs[index-baseIndex].Term, Index: index})
	for i := index + 1; i <= lastIndex; i++ {
		newLogs = append(newLogs, rf.logs[i-baseIndex])
	}
	rf.logs = newLogs
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogs[0].Index)
	e.Encode(newLogs[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}
