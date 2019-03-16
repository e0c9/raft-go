package shardmaster

import (
	"fmt"
	"raft-go/raft"
	"strconv"
	"time"
)
import "raft-go/labrpc"
import "sync"
import "encoding/gob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	request map[int64]int
	result  map[int]chan Op

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type    string
	Cid int64
	Seq int
	Command interface{}
}

func (sm *ShardMaster) appendLog(op Op) bool {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	sm.mu.Lock()
	// note：can not use `make(chan op)`
	sm.result[index] = make(chan Op, 1)
	sm.mu.Unlock()

	select {
	case cmd := <-sm.result[index]:
		sm.mu.Lock()
		sm.request[op.Cid] = op.Seq
		sm.mu.Unlock()
		return cmd == op
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.Type = "JOIN"
	op.Cid = args.Cid
	op.Seq = args.Seq
	op.Command = args

	if !sm.appendLog(op) {
		reply.WrongLeader = true
		reply.Err = "This node is not leader."
		return
	}
	reply.WrongLeader = false
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.Type = "LEAVE"
	op.Command = args
	op.Cid = args.Cid
	op.Seq = args.Seq

	if !sm.appendLog(op) {
		reply.WrongLeader = true
		reply.Err = "This node is not leader"
		return
	}
	reply.WrongLeader = false
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.Type = "MOVE"
	op.Command = args
	op.Cid = args.Cid
	op.Seq = args.Seq

	if !sm.appendLog(op) {
		reply.WrongLeader = true
		reply.Err = "This node is not leader"
		return
	}
	reply.WrongLeader = false
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Type = "QUERY"
	op.Command = args
	op.Cid = args.Cid
	op.Seq = args.Seq

	if !sm.appendLog(op) {
		reply.WrongLeader = true
		reply.Err = "This node is not leader"
		return
	}

	sm.mu.Lock()
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	sm.mu.Unlock()
	reply.WrongLeader = false
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) runServer() {
	for {
		msg := <-sm.applyCh
		op, _ := msg.Command.(Op)

		switch op.Type {
		case "JOIN":
			joinArgs := op.Command.(*JoinArgs)
			fmt.Println("收到Join")
			sm.mu.Lock()
			if sm.checkDuplicate(joinArgs.Cid, joinArgs.Seq) {
				fmt.Println("进入 Join 成功")
				config := sm.newConfig()
				for gid, servers := range joinArgs.Servers {
					fmt.Println("Join " + strconv.Itoa(gid))
					config.Groups[gid] = servers
				}
				sm.updateConfig(config)
				sm.success(msg.Index, op)
			}
			sm.mu.Unlock()
		case "LEAVE":
			leaveArgs := op.Command.(*LeaveArgs)
			fmt.Println("收到 Leave")
			sm.mu.Lock()
			if sm.checkDuplicate(leaveArgs.Cid, leaveArgs.Seq) {
				fmt.Println("leave 成功进入")
				config := sm.newConfig()
				for _, gid := range leaveArgs.GIDs {
					fmt.Println("Leave " + strconv.Itoa(gid))
					delete(config.Groups, gid)
				}
				sm.updateConfig(config)
				sm.success(msg.Index, op)
			}
			sm.mu.Unlock()
		case "MOVE":
			moveArgs := op.Command.(*MoveArgs)
			sm.mu.Lock()
			if sm.checkDuplicate(moveArgs.Cid, moveArgs.Seq) {
				config := sm.newConfig()
				config.Shards[moveArgs.Shard] = moveArgs.GID
				sm.configs = append(sm.configs, config)
				sm.success(msg.Index, op)
			}
			sm.mu.Unlock()
		case "QUERY":
			queryArgs := op.Command.(*QueryArgs)
			sm.mu.Lock()
			fmt.Println("Query 请求序号：" + strconv.Itoa(queryArgs.Seq) + " 请求配置编号：" +
				strconv.Itoa(queryArgs.Num) + " 现有的配置文件： " + strconv.Itoa(len(sm.configs)) + " 现在序号 " + strconv.Itoa(sm.request[queryArgs.Cid]))
			if sm.checkDuplicate(queryArgs.Cid, queryArgs.Seq) {
				sm.success(msg.Index, op)
			}
			sm.mu.Unlock()

		}
	}
}

func (sm *ShardMaster) newConfig() Config {
	oldConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Groups = make(map[int][]string)
	for k, v := range oldConfig.Groups {
		tmp := make([]string, len(v))
		copy(tmp, v)
		newConfig.Groups[k] = tmp
	}
	return newConfig
}

func (sm *ShardMaster) success(index int, op Op) {
	ch, ok := sm.result[index]
	if ok {
		ch <- op
	}
}

func (sm *ShardMaster) updateConfig(config Config) {
	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	g2s := make(map[int][]int)
	var shards []int
	for shard, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			shards = append(shards, shard)
		} else {
			g2s[gid] = append(g2s[gid], shard)
		}
	}
	if len(shards) != 0 {
		for i, j := 0, 0; i < len(shards); i++ {
			if j == len(gids) {
				j = 0
			}
			config.Shards[shards[i]] = gids[j]
			j++
		}
	} else {
		expected := len(shards) / len(gids)
		var moveShards []int
		var newJoin int
		for k, v := range g2s {
			if len(v) > expected {
				for i := 0; i < len(v)-expected; i++ {
					moveShards = append(moveShards, v[i])
				}
			} else if len(v) == 0 {
				newJoin = k
			}
		}
		for shard := range moveShards {
			config.Shards[shard] = newJoin
		}
	}

	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) checkDuplicate(cid int64, seq int) bool {
	if lastSeq, ok := sm.request[cid]; ok {
		return seq > lastSeq
	}
	return true

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.result = make(map[int]chan Op)
	sm.request = make(map[int64]int)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	go sm.runServer()

	return sm
}
