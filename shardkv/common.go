package shardkv

import "raft-go/shardmaster"

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	// You'll have to add definitions here
	Cid int64
	Seq int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardArgs struct {
	Shards []int
	CfgNum int
}

type GetShardReply struct {
	WrongLeader bool
	Err         Err
	Content     map[string]string
	TaskSeq     map[int64]int
}

type ReconfigureArgs struct {
	Cfg     shardmaster.Config
	Content map[string]string
	TaskSeq map[int64]int
}

type ReconfigureReply struct {
	Err Err
}
