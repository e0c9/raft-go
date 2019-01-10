package raftkv

import (
	"crypto/rand"
	"math/big"
	"raft-go/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// modify this struct
	leaderIndex int
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
	ck.leaderIndex = 0
	return ck
}

// return "" if the key does not exist
//
// send RPC; ok := ck.server[i].Call("RaftKV.Get",&args, &reply)
func (ck *Clerk) Get(key string) string {
	DPrintf("send a Get request: key(%v)", key)
	args := &GetArgs{}
	args.Key = key
	reply := &GetReply{}

	idx := ck.leaderIndex
	for {
		if idx == len(ck.servers) {
			idx = 0
		}
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
	DPrintf("send a Put request: key(%v)->value(%v)", key, value)
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	reply := &PutAppendReply{}

	idx := ck.leaderIndex
	for {
		if idx == len(ck.servers) {
			idx = 0
		}
		ok := ck.servers[idx].Call("RaftKV.PutAppend", args, reply)
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
