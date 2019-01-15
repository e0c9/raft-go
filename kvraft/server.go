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
	Uuid  int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// definitions here
	store    map[string]string
	response map[int64]chan Result
}

type Result struct {
	value string
	err   Err
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("📨 server receve a Get request key[%v]", args.Key)
	op := Op{}
	op.Key = args.Key
	op.Type = "Get"
	op.Uuid = args.Uuid
	reply.WrongLeader = false
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.response[op.Uuid] = make(chan Result, 1)
	kv.mu.Unlock()

	select {
	case res := <-kv.response[op.Uuid]:
		reply.Err = res.err
		reply.Value = res.value
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("📨 server receive PutAppend request")
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.Uuid = args.Uuid
	reply.WrongLeader = false

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.response[op.Uuid] = make(chan Result)
	select {
	case res := <-kv.response[op.Uuid]:
		reply.Err = res.err
	case <-time.After(2 * time.Second):
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func (kv *RaftKV) runServer() {
	for {
		msg := <-kv.applyCh
		op, _ := msg.Command.(Op)
		res := Result{}

		kv.mu.Lock()
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
		DPrintf("🔚 [%v] operation finished: res.value[%v] res.err[%v]", op.Type, res.value, res.err)
		ch, ok := kv.response[op.Uuid]

		if ok {
			ch <- res
		}
		kv.mu.Unlock()

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
	kv.response = make(map[int64]chan Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// initialization code here
	go kv.runServer()

	return kv
}
