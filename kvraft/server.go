package raftkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"raft-go/labrpc"
	"raft-go/raft"
	"sync"
	"time"
)

const Debug = 0

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
	Seq   int
	Cid   int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// definitions here
	store   map[string]string
	request map[int64]int
	result  map[int]chan Op
}

func (kv *RaftKV) appendLog(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	// note：can not use `make(chan op)`
	kv.result[index] = make(chan Op, 1)
	kv.mu.Unlock()

	select {
	case cmd := <-kv.result[index]:
		return cmd == op
	case <-time.After(200 * time.Millisecond):
		return false
	}

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("📨 server receve a Get request key[%v]", args.Key)
	op := Op{}
	op.Key = args.Key
	op.Type = "Get"

	ok := kv.appendLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	value, exist := kv.store[op.Key]
	kv.request[args.Cid] = args.Seq
	kv.mu.Unlock()

	if exist {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("📨 server receive PutAppend request")
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.Cid = args.Cid
	op.Seq = args.Seq

	ok := kv.appendLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK

}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func (kv *RaftKV) runServer() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			data := msg.Snapshot
			var lastIncludedIndex int
			var lastIncludedTerm int
			r := bytes.NewBuffer(data)
			d := gob.NewDecoder(r)
			kv.mu.Lock()
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			kv.store = make(map[string]string)
			kv.request = make(map[int64]int)
			d.Decode(&kv.store)
			d.Decode(&kv.request)
			kv.mu.Unlock()
		}
		op, _ := msg.Command.(Op)

		kv.mu.Lock()
		if seq, ok := kv.request[op.Cid]; !ok || op.Seq > seq {
			kv.execute(op)
			kv.request[op.Cid] = op.Seq
		}

		ch, ok := kv.result[msg.Index]

		if ok {
			ch <- op
		}
		if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
			buf := new(bytes.Buffer)
			e := gob.NewEncoder(buf)
			e.Encode(&kv.store)
			e.Encode(&kv.request)
			go kv.rf.StartSnapshot(buf.Bytes(), msg.Index)
		}
		kv.mu.Unlock()

	}
}

func (kv *RaftKV) execute(op Op) {
	switch op.Type {
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
}

// StartKVServer() must retuen quickly, so it should start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// initialization code here
	kv.store = make(map[string]string)
	kv.request = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// initialization code here
	go kv.runServer()

	return kv
}
