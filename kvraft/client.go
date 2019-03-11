package raftkv

import (
	"crypto/rand"
	"math/big"
	"raft-go/labrpc"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// modify this struct
	leader int
	cid    int64
	seq    int
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

// return "" if the key does not exist
//
// send RPC; ok := ck.server[i].Call("RaftKV.Get",&args, &reply)
func (ck *Clerk) Get(key string) string {
	DPrintf("DEBUG: send a Get request: key(%v)", key)
	args := &GetArgs{}
	args.Key = key
	args.Cid = ck.cid
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()

	idx := ck.leader
	for {
		if idx == len(ck.servers) {
			idx = 0
		}
		DPrintf("✉️ Send a Get request to server[%v]:key(%v)", idx, key)
		reply := &GetReply{}
		ok := ck.servers[idx].Call("RaftKV.Get", args, reply)
		if ok && !reply.WrongLeader {
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value
		}
		idx++
	}
}

// shared by Put and Append
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	args.Cid = ck.cid
	args.Op = op
	idx := ck.leader
	for {
		if idx == len(ck.servers) {
			idx = 0
		}
		DPrintf("✉️ send a Put request to server[%v]: cid[%v]idx[%v] key(%v)->value(%v)", idx, args.Cid, args.Seq, key, value)

		reply := &PutAppendReply{}
		ok := ck.servers[idx].Call("RaftKV.PutAppend", args, reply)
		DPrintf("ok[%v] wrongLeader[%v]", ok, reply.WrongLeader)
		if ok && !reply.WrongLeader {
			return
		}
		idx++
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
