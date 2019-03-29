package shardkv

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
	Type          string
	GetArgs       GetArgs
	PutAppendArgs PutAppendArgs
	ReCfg         ReconfigureArgs
}

type Result struct {
	args  interface{}
	reply interface{}
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
	muRes   sync.Mutex
	sm      *shardmaster.Clerk
	config  shardmaster.Config
	store   map[string]string
	request map[int64]int
	result  map[int]chan Result
}

func (kv *ShardKV) staleTask(cid int64, seq int) bool {
	if lastseq, ok := kv.request[cid]; ok && lastseq >= seq {
		return true
	}
	kv.request[cid] = seq
	return false
}

func (kv *ShardKV) checkKey(key string) bool {
	sid := key2shard(key)
	return kv.gid == kv.config.Shards[sid]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	idx, _, isLeader := kv.rf.Start(Op{Type: "Get", GetArgs: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.muRes.Lock()
	ch, ok := kv.result[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.result[idx] = ch
	}
	kv.muRes.Unlock()

	select {
	case msg := <-ch:
		if ret, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Cid != ret.Cid || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	idx, _, isLeader := kv.rf.Start(Op{Type: "PutAppend", PutAppendArgs: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.muRes.Lock()
	ch, ok := kv.result[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.result[idx] = ch
	}
	kv.muRes.Unlock()

	select {
	case res := <-ch:
		if ret, ok := res.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Cid != ret.Cid || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				reply.Err = res.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <-time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}

}

// little different
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Err = OK
	reply.Request = make(map[int64]int)
	reply.Content = make(map[string]string)
	for k, v := range kv.store {
		shard := key2shard(k)
		for _, sid := range args.Shards {
			if shard == sid {
				reply.Content[k] = v
				//break
			}
		}
	}

	for cid := range kv.request {
		reply.Request[cid] = kv.request[cid]
	}
}

func (kv *ShardKV) syncConfigure(args ReconfigureArgs) bool {
	idx, _, isLeader := kv.rf.Start(Op{Type: "Reconfigure", ReCfg: args})
	if !isLeader {
		return false
	}
	kv.muRes.Lock()
	ch, ok := kv.result[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.result[idx] = ch
	}
	kv.muRes.Unlock()
	select {
	case msg := <-ch:
		if ret, ok := msg.args.(ReconfigureArgs); !ok {
			return ret.Cfg.Num == args.Cfg.Num
		}
	case <-time.After(400 * time.Millisecond):
		return false
	}
	return false
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) readSnapshot(data []byte) {
	var lastIncludedIndex int
	var lastIncludedTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.mu.Lock()
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.store)
	d.Decode(&kv.request)
	d.Decode(&kv.config)
	kv.mu.Unlock()
}

func (kv *ShardKV) checkSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
		buf := new(bytes.Buffer)
		e := gob.NewEncoder(buf)
		e.Encode(kv.store)
		e.Encode(kv.request)
		e.Encode(kv.config)
		go kv.rf.StartSnapshot(buf.Bytes(), index)
	}
}

func (kv *ShardKV) runServer() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			kv.readSnapshot(msg.Snapshot)
		} else {
			op := msg.Command.(Op)
			var result Result
			switch op.Type {
			case "Get":
				//raft.DPrintf("收到 GET 请求")
				result.args = op.GetArgs
				result.reply = kv.applyGet(op.GetArgs)
			case "PutAppend":
				//raft.DPrintf("收到 PUT 请求")
				result.args = op.PutAppendArgs
				result.reply = kv.applyPutAppend(op.PutAppendArgs)
			case "Reconfigure":
				//raft.DPrintf("收到重新配置请求")
				result.args = op.ReCfg
				result.reply = kv.applyReconfigure(op.ReCfg)
			}
			kv.success(msg.Index, result)
			kv.checkSnapshot(msg.Index)
		}
	}
}

func (kv *ShardKV) success(index int, result Result) {
	kv.muRes.Lock()
	defer kv.muRes.Unlock()

	if _, ok := kv.result[index]; !ok {
		kv.result[index] = make(chan Result, 1)
	} else {
		select {
		case <-kv.result[index]:
		default:
		}
	}
	kv.result[index] <- result
}

func (kv *ShardKV) applyGet(args GetArgs) (reply GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	if value, ok := kv.store[args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	return
}

func (kv *ShardKV) applyPutAppend(args PutAppendArgs) (reply PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	if !kv.staleTask(args.Cid, args.Seq) {
		if args.Op == "Put" {
			kv.store[args.Key] = args.Value
		} else {
			kv.store[args.Key] += args.Value
		}
	}
	reply.Err = OK
	//if seq, ok := kv.request[args.Cid]; !ok || args.Seq > seq {
	//	kv.request[args.Cid] = args.Seq
	//	switch args.Op {
	//	case "Put":
	//		kv.store[args.Key] = args.Value
	//	case "Append":
	//		v, ok := kv.store[args.Key]
	//		if ok {
	//			kv.store[args.Key] = v + args.Value
	//		} else {
	//			kv.store[args.Key] = args.Value
	//		}
	//	}
	//}
	//reply.Err = OK
	return
}

func (kv *ShardKV) applyReconfigure(args ReconfigureArgs) (reply ReconfigureReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Cfg.Num > kv.config.Num {
		for k, v := range args.Content {
			kv.store[k] = v
		}

		for cid := range args.Request {
			if seq, exist := kv.request[cid]; !exist || seq < args.Request[cid] {
				kv.request[cid] = args.Request[cid]
			}
		}
		kv.config = args.Cfg
		reply.Err = OK
	}
	return
}

func (kv *ShardKV) watchConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
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
	args.Request = make(map[int64]int)
	args.Content = make(map[string]string)

	var wait sync.WaitGroup
	var mu sync.Mutex
	for gid, sids := range mergeShards {
		wait.Add(1)
		go func(gid int, sids []int) {
			defer wait.Done()
			var reply GetShardReply
			if kv.pullShard(gid, &GetShardArgs{Shards: sids, CfgNum: conf.Num}, &reply) {
				mu.Lock()
				for k, v := range reply.Content {
					args.Content[k] = v
				}

				for cid := range reply.Request {
					if seq, exist := args.Request[cid]; !exist && seq < reply.Request[cid] {
						args.Request[cid] = reply.Request[cid]
					}
				}
				mu.Unlock()
			} else {
				ok = false // has problem
			}
		}(gid, sids)
	}
	wait.Wait()
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
	kv.result = make(map[int]chan Result)

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.config = kv.sm.Query(-1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.runServer()
	go kv.watchConfig()

	return kv
}
