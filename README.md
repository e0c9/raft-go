### Raft一致性协议
#### 介绍
该项目来自于MIT 6.824分布式系统的课程，最终实现一个分布式KV存储。
* 实现 raft 协议的 leader 选举，一个复制状态机协议
* 实现 raft 协议中的日志复制
* 基于 raft 协议在其上实现分布KV存储服务

#### raft 介绍
raft 协议用于管理出现错误时必须继续运行的副本服务器集群，其中挑战是副本服务器并不总是保持相同的数据，raft 协议有助于理清正确的数据。

raft 的基本方法是实现一个复制状态机，将客户端的请求组织为一个线性的日志，并且确保所有的副本服务器在日志内容上达成一致。每一个副本服务器按照日志中记录的顺序执行客户端的请求。因为所有在线的副本看见相同的日志内容，所以他们都按照相同的顺序执行请求，从而达到一致的服务状态。如果一个服务出现故障后恢复，raft 会将日志更新到最新的内容。raft 将继续执行只要超过半数的服务器是处于运行状态且可以相互通信。如果没有大多数服务器在线，raft 将停止工作，但一旦大多数存活，它就会从停止的地方重新开始运行。

更多参考：Paxos，Chubby， Paxos Made Live，Spanner， Zookeeper， Harp， Viewstamped
Replication Bolosky

#### 实现 leader 选举
* 补充 RequestVoteArgs 和 RequestVoteReply
* 修改 Make() 创建一个协程在背后开始选举（当没有感知到其他raft节点时）
* 实现 RequestVote() rpc 请求 
* 实现心跳检测，定义 AppendEntriers Rpc 结构体。让 leader 定期发送它
* 实现AppendEntries RPC处理方法 （重设选举时间，防止服务器在具有leader时继续选举）
```go
// create a new Raft server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
rf.Start(command interface{}) (index, term, isleader)

// ask a Raft for its current term, and whether it thinks it is leader
rf.GetState() (term, isLeader)

// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester).
type ApplyMsg
```

执行领导人选举和心跳 (空的附录) 应该足以让一个领导人当选, 在没有失败的情况下, 应该继续担任领导人, 并在失败后重新确定领导者。

测试脚本
```bash
$ go test -run 2A
Test: initial election ...
  ... Passed
Test: election after network failure ...
  ... Passed
PASS
  ok  raft7.008s
```