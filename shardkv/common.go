package shardkv

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
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
