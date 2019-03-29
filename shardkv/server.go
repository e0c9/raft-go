package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"raft-go/labrpc"
	"raft-go/raft"
	"raft-go/shardmaster"
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
	ReCfg ReconfigureArgs
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
	sm      *shardmaster.Clerk
	config  shardmaster.Config
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

func (kv *ShardKV) checkKey(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sid := key2shard(key)
	return kv.gid == kv.config.Shards[sid]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if _, leader := kv.rf.GetState(); !leader {
		reply.WrongLeader = true
		return
	}
	if !kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		raft.DPrintf("GET 拒绝服务，该请求不属于该副本组")
		return
	}
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
	if _, leader := kv.rf.GetState(); !leader {
		reply.WrongLeader = true
		return
	}
	if !kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		raft.DPrintf("PutAppend 拒绝请求，不属于该副本组")
		return
	}
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

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Err = OK
	reply.TaskSeq = make(map[int64]int)
	reply.Content = make(map[string]string)
	for k, v := range kv.store {
		for _, sid := range args.Shards {
			if key2shard(k) == sid {
				reply.Content[k] = v
				break
			}
		}
	}

	for cid := range kv.request {
		reply.TaskSeq[cid] = kv.request[cid]
	}
}

func (kv *ShardKV) syncConfigure(args ReconfigureArgs) bool {
	raft.DPrintf("syncConfigure 同步更新配置")
	idx, _, isLeader := kv.rf.Start(Op{Type: "Reconfigure", ReCfg: args})
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.result[idx]
	if !ok {
		ch = make(chan bool, 1)
		kv.result[idx] = ch
	}
	kv.mu.Unlock()
	select {
	case <-ch:
		raft.DPrintf("更新配置成功")
		return true
	case <-time.After(400 * time.Millisecond):
		return false
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
		if op.Type == "Reconfigure" {
			raft.DPrintf("收到配置更新操作")
			kv.applyReconfigure(op.ReCfg)
		} else {
			if seq, ok := kv.request[op.Cid]; !ok || op.Seq > seq {
				kv.execute(op)
			}
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

func (kv *ShardKV) applyReconfigure(args ReconfigureArgs) {
	if args.Cfg.Num > kv.config.Num {
		for k, v := range args.Content {
			kv.store[k] = v
		}

		for cli := range args.TaskSeq {
			if seq, exist := kv.request[cli]; !exist || seq < args.TaskSeq[cli] {
				kv.request[cli] = args.TaskSeq[cli]
			}
		}
		kv.config = args.Cfg
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

func (kv *ShardKV) watchConfig() {
	for {
		if _, leader := kv.rf.GetState(); leader {
			conf := kv.sm.Query(-1)
			for i := kv.config.Num + 1; i <= conf.Num; i++ {
				if !kv.processReConf(kv.sm.Query(i)) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) processReConf(conf shardmaster.Config) bool {
	raft.DPrintf("%v", conf)
	ok := true
	mergeShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		// new shards need to serve
		if conf.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := mergeShards[gid]; !ok {
					mergeShards[gid] = []int{i}
				} else {
					mergeShards[gid] = append(mergeShards[gid], i)
				}
			}
		}
	}
	args := ReconfigureArgs{Cfg: conf}
	args.TaskSeq = make(map[int64]int)
	args.Content = make(map[string]string)

	var wait sync.WaitGroup
	for gid, sids := range mergeShards {
		wait.Add(1)
		go func(gid int, sids []int) {
			defer wait.Done()
			var reply GetShardReply
			if kv.pullShard(gid, &GetShardArgs{Shards: sids, CfgNum: conf.Num}, &reply) {
				for k, v := range reply.Content {
					args.Content[k] = v
				}

				for cid := range reply.TaskSeq {
					if seq, exist := args.TaskSeq[cid]; !exist && seq < reply.TaskSeq[cid] {
						args.TaskSeq[cid] = reply.TaskSeq[cid]
					}
				}
			} else {
				ok = false
			}
		}(gid, sids)
	}
	wait.Wait()
	raft.DPrintf("更新完毕")
	return ok && kv.syncConfigure(args)
}

func (kv *ShardKV) pullShard(gid int, args *GetShardArgs, reply *GetShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.GetShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
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
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.sm.Query(-1)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.runServer()
	go kv.watchConfig()

	return kv
}
