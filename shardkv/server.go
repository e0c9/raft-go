package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"raft-go/labrpc"
	"raft-go/raft"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Cid   int64
	Seq   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string
	request map[int64]int
	result  map[int]chan bool
}

func (kv *ShardKV) appendLog(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	kv.result[index] = make(chan bool, 1)
	kv.mu.Unlock()

	select {
	case ok := <-kv.result[index]:
		kv.mu.Lock()
		kv.request[op.Cid] = op.Seq
		kv.mu.Unlock()
		return ok
	case <-time.After(800 * time.Millisecond):
		return false
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Key = args.Key
	op.Type = "Get"
	reply.WrongLeader = false
	if ok := kv.appendLog(op); !ok {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if value, exist := kv.store[op.Key]; exist {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.Cid = args.Cid
	op.Seq = args.Seq

	if ok := kv.appendLog(op); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) runServer() {
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
			d.Decode(&kv.store)
			d.Decode(&kv.request)
			kv.mu.Unlock()
		}
		op, _ := msg.Command.(Op)
		kv.mu.Lock()
		if seq, ok := kv.request[op.Cid]; !ok || op.Seq > seq {
			kv.execute(op)
		}

		ch, ok := kv.result[msg.Index]
		if ok {
			ch <- true
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

func (kv *ShardKV) execute(op Op) {
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

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.store = make(map[string]string)
	kv.request = make(map[int64]int)
	kv.result = make(map[int]chan bool)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.runServer()

	return kv
}
