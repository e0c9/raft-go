package raftkv

import (
	"encoding/gob"
	"log"
	"raft-go/labrpc"
	"raft-go/raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// difinitions here
	Type  string
	Key   string
	Value string
	Index int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// definitions here
	store    map[string]string
	response map[int]chan Result
}

type Result struct {
	value string
	err   Err
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server receve a Get request")
	op := Op{}
	op.Key = args.Key
	op.Type = "Get"
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	kv.response[index] = make(chan Result, 1)
	kv.mu.Unlock()
	select {
	case res := <-kv.response[index]:
		reply.Err = res.err
		reply.Value = res.value
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server receive PutAppend request")
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintf("BOOM BOOM BOOM, this server is a Leader.")
	reply.WrongLeader = false
	kv.response[index] = make(chan Result)
	DPrintf("index 值为 %v", index)
	select {
	case res := <-kv.response[index]:
		DPrintf("收到结果了 为什么出问题")
		reply.Err = res.err
	case <-time.After(10 * time.Second):
		DPrintf("=======================================")
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// code here
}

func (kv *RaftKV) runServer() {
	DPrintf("start run kv server....")
	for {
		msg := <-kv.applyCh
		DPrintf("收到 信息， 可以进行操作了")
		op, _ := msg.Command.(Op)
		res := Result{}

		switch op.Type {
		case "Get":
			v, ok := kv.store[op.Key]
			if ok {
				res.value = v
				res.err = OK
			} else {
				res.err = ErrNoKey
			}
		case "Put":
			kv.store[op.Key] = op.Value
		case "Append":
			v, ok := kv.store[op.Key]
			if ok {
				kv.store[op.Key] = v + op.Value
			} else {
				kv.store[op.Key] = op.Value
			}
		}
		DPrintf("操作结束了， debug de 的我想哭")
		kv.response[op.Index] <- res

	}
}

// StartKVServer() must retuen quickly, so it should start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// initialization code here
	kv.store = make(map[string]string)
	kv.response = make(map[int]chan Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// initialization code here
	go kv.runServer()

	return kv
}
