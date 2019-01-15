package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// add definitions here
	Uuid int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// add definitions here
	Uuid int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
