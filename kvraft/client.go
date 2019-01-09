package raftkv

import (
	"crypto/rand"
	"math/big"
	"raft-go/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// modify this struct
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
	// code here
	return ck
}

// return "" if the key does not exist
//
// send RPC; ok := ck.server[i].Call("RaftKV.Get",&args, &reply)
func (ck *Clerk) Get(key string) string {
	// modify this function
	return ""
}

// shared by Put and Append
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// modify this function
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
