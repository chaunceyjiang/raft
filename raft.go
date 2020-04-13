package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var DebugCM = 0

// raft 协议实现

// 角色类型
type StateType uint64

const (
	Follower StateType = iota
	Candidate
	Leader
	Dead
)

var stmap = [...]string{
	"Follower",
	"Candidate",
	"Leader",
	"Dead",
}

func (st StateType) String() string {
	return stmap[st]
}

// 实现论文中一致性模块
type ConsensusModule struct {

	// 必备条件
	mu sync.Mutex

	id uint64 // 节点id

	peerIds []uint64 // 整个集群的其他端点id

	state StateType //角色

	// rpc 服务
	server *Server

	// 论文描述
	// Persistent Raft state on all servers
	currentTerm uint64     // 当前任期
	voteFor     uint64     // 投票标记
	log         []LogEntry // 日志

	// 论文描述
	// Volatile Raft state on all servers
	commitIndex uint64
	lastApplied uint64

	// 论文描述
	// Volatile Raft state on leaders
	// 这两个数组只对 Leader 有用
	nextIndex  map[uint64]uint64 // 保存要发送给每一个Follower的下一个日志条目
	matchIndex map[uint64]uint64 // 跟踪Leader和每个Follower匹配到的日志条目

	// 各种计时器
	// 选举超时
	electionTime time.Time
	// 心跳
	HeartbeatTimeDuration time.Duration
}

const None uint64 = 0

func NewConsensusModule(id uint64, peerIds []uint64, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := &ConsensusModule{
		mu:          sync.Mutex{},
		id:          0,
		peerIds:     peerIds,
		state:       Follower,
		server:      server,
		currentTerm: 0,    // 根据论文，初始化 0
		voteFor:     None, //根据论文 初始化 None
	}
	go func() {
		// 接收到一个ready信号，则选举计时器开始
		<-ready

		cm.mu.Lock()
		cm.electionTime = time.Now()
		cm.mu.Unlock()

		cm.runElectionTimer()
	}()
	return cm
}

// debugLog 调试信息
func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// electionTimeout 生成一个随机时间
// 论文描述该时间为 150ms-300ms
func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		// 测试，硬编码选举时间
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// runElectionTimer 实现一个选举计时器
func (cm *ConsensusModule) runElectionTimer() {
	// 生成一个选举超时时间
	timeoutDuration := cm.electionTimeout()

	cm.mu.Lock()
	termStarted := cm.currentTerm // 记录选举计时器开始的任期，当计时器发现任期变化时，计时器退出
	cm.mu.Unlock()

	// 这里每5毫秒检查一次
	//
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			// 不是候选者，也不是跟随者
			//  因此选举计时器，不需要
			cm.mu.Unlock()
			return // 选举计时器退出
		}

		if termStarted != cm.currentTerm {
			// 在选举计时器，及时期间，任期发生变化
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionTime); elapsed >= timeoutDuration {
			// 选举超时
			// 开始进行选举
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		// 没有超时
		// 选举计时器，等待一个ticker时间继续检查
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	// 整个 startElection 都在排斥锁中，因此这个这个函数不加锁
	cm.state = Candidate // 身份改变，变为候选者
	cm.currentTerm += 1  //任期递增，每一个任期最多只有个领导者
	savedCurrentTerm := cm.currentTerm
	cm.electionTime = time.Now() // 更新选举计时器时间
	cm.voteFor = cm.id           // 将票投给自己
	cm.debugLog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// 统计的自己收到的投票个数
	var votesCount uint32

	for _, peerId := range cm.peerIds {
		// 并发的发起 RequestVote RPC
		go func(peerId uint64) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply
			cm.debugLog("sending RequestVote to %d: %+v", peerId, args)
			// 发起一个RPC Call
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				// 统计活的的投票个数
				cm.mu.Lock() //阻塞
				defer cm.mu.Unlock()
				cm.debugLog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					// 在RPC期间 自己的角色已经发生了改变
					return //直接退出
				}

				if reply.Term > savedCurrentTerm {
					// 如果回复的任期大于自己的任期，表示有新的已经开始了，新的一轮，一次自己直接变成跟随者
					cm.becomeFollower(reply.Term)
					// 这里有一个隐藏
					// 当接收到别自己到的任期后，变成跟随者时，上一个任期的选举计时器也会失效
					return
				}

				if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						// 任期是自己所在的任期，且有投票给自己
						votes := atomic.AddUint32(&votesCount, 1) //个数加一
						if votes*2 > uint32(len(cm.peerIds))+1 {
							// 获取大多数的投票
							// 变成领导者，选举计时器会退出
							cm.debugLog("id %d wins election with %d votes", cm.id, votes)
							cm.becomeLeader()
						}
					}
				}
			}
		}(peerId)
	}
	// 开始一个新的选举计时器，上一个计时器会退出
	go cm.runElectionTimer()
}

// 变成追随者
func (cm *ConsensusModule) becomeFollower(term uint64) {
	// 依赖函数外部锁
	cm.debugLog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.voteFor = None
	cm.electionTime = time.Now()

	go cm.runElectionTimer() // 变成跟随者，则开始一个新任期的选举计时器
}

// 变成领导者
func (cm *ConsensusModule) becomeLeader() {
	cm.state = Leader
	cm.debugLog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		// 心跳计时器
		ticker := time.NewTicker(cm.HeartbeatTimeDuration)
		defer ticker.Stop()

		for {
			// 发送心跳信息
			cm.leaderSendHeartbeats()
			<-ticker.C
			cm.mu.Lock()
			if cm.state != Leader {
				// 直到不是领导者，退出
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// leaderSendHeartbeats 将心跳信息发送给所有的节点，收集他们的回复信息
// 论文中 信息信息跟日志追加相同
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Lock()

	for _, peerId := range cm.peerIds {
		// 论文中说明，如果日志追加是空，则表示为心跳信息
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(peerId uint64) {
			cm.debugLog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					// 心跳信息中的任期大于当前任期,则变为跟随者
					cm.debugLog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					// 同时 心跳计时器退出
					return
				}
			}
		}(peerId)
	}
}

// 实现RPC RequestVote
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.debugLog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.voteFor)
	// 开始投票
	if args.Term > cm.currentTerm {
		// 论文实现
		// 收到的投票请求中的任期大于自己的任期，自己变为追随者
		cm.debugLog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	canVote := args.Term == cm.currentTerm &&
		(cm.voteFor == None || cm.voteFor == args.CandidateId)
	// 论文实现
	if canVote {
		// 可以正常投票
		cm.voteFor = args.CandidateId // 投票
		cm.electionTime = time.Now()  // 更新选举计时器
		reply.VoteGranted = true      // 投票
	} else {
		reply.VoteGranted = false // 不能正常投票
	}

	reply.Term = cm.currentTerm
	cm.debugLog("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntries 实现日志追加
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.debugLog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false

	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			// 收到其他节点的信息，自己变成跟随者
			cm.becomeFollower(args.Term)
		}
		cm.electionTime = time.Now() // 更新选举计时器
		reply.Success = true         // 更新成功
	}
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}
