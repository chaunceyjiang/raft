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

var DebugCM = 2

// raft 协议实现

// 角色类型
type StateType int

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

	id int // 节点id

	peerIds []int // 整个集群的其他端点id

	state StateType //角色

	// rpc 服务
	server *Server

	// 论文描述
	// Persistent Raft state on all servers
	currentTerm int        // 当前任期
	voteFor     int        // 投票标记
	log         []LogEntry // 日志

	// 论文描述
	// Volatile Raft state on all servers
	commitIndex int
	lastApplied int

	// 论文描述
	// Volatile Raft state on leaders
	// 这两个数组只对 Leader 有用
	// 领导人针对每一个跟随者维护了一个 nextIndex，这表示下一个需要发送给跟随者的日志条目的索引地址。当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1
	nextIndex  map[int]int // 保存要发送给每一个Follower的下一个日志条目,论文5.3
	matchIndex map[int]int // 跟踪Leader和每个Follower匹配到的日志条目

	// 各种计时器
	// 选举超时
	electionTime time.Time
	// 心跳
	HeartbeatTimeDuration time.Duration

	// 与客户端进行通信
	newCommitReadyChan chan struct{}
	commitChan         chan<- CommitEntry
}

const None int = -1

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := &ConsensusModule{
		mu:                    sync.Mutex{},
		id:                    id,
		peerIds:               peerIds,
		state:                 Follower,
		server:                server,
		currentTerm:           0,    // 根据论文，初始化 0
		voteFor:               None, //根据论文 初始化 None
		HeartbeatTimeDuration: 30 * time.Millisecond,
		nextIndex:             make(map[int]int),
		matchIndex:            make(map[int]int),
		lastApplied:           None,
		commitIndex:           None,
		newCommitReadyChan:    make(chan struct{}, 16),
		commitChan:            commitChan,
	}
	go func() {
		// 接收到一个ready信号，则选举计时器开始
		<-ready
		cm.debugLog("初始计时器开始计时")
		cm.mu.Lock()
		cm.electionTime = time.Now()
		cm.mu.Unlock()

		cm.runElectionTimer()
	}()
	go cm.commitChanSender()
	go cm.logReport()
	return cm
}

// debugLog 调试信息
func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}
func (cm *ConsensusModule) logReport() {
	if DebugCM > 0 {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			cm.mu.Lock()
			if cm.state == Dead {
				break
			}
			cm.debugLog("%s all log %v", cm.state.String(), cm.log)
			cm.mu.Unlock()
			<-ticker.C
		}
	}

}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	close(cm.newCommitReadyChan) // 关闭newCommitReadyChan ,否则commitChanSender会泄露
	cm.debugLog("becomes Dead")
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
	cm.debugLog("获得一个选举超时时间 %v", timeoutDuration)
	termStarted := cm.currentTerm // 记录选举计时器开始的任期，当计时器发现任期变化时，计时器退出
	cm.mu.Unlock()

	// 这里每5毫秒检查一次
	//
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			// 不是候选者，也不是跟随者
			//  因此选举计时器，不需要
			cm.mu.Unlock()
			cm.debugLog("选举计时器退出 state %s", cm.state.String())
			return // 选举计时器退出
		}

		if termStarted != cm.currentTerm {
			// 在选举计时器，及时期间，任期发生变化
			cm.mu.Unlock()
			cm.debugLog("选举计时器退出 currentTerm %d termStarted %d ", cm.currentTerm, termStarted)
			return
		}
		// 每一次心跳Follower 收到心跳信息都会刷新electionTime,如果长时间没有收到心跳信息,自己就会开始进入下一任的选举.
		if elapsed := time.Since(cm.electionTime); elapsed >= timeoutDuration {
			cm.debugLog("选举计时 electionTime %v elapsed %v timeoutDuration %v ", cm.electionTime, elapsed, timeoutDuration)
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

	// 统计的自己收到的投票个数,自己给自己投了一票
	var votesCount uint32 = 1

	for _, peerId := range cm.peerIds {
		// 并发的发起 RequestVote RPC
		go func(peerId int) {
			cm.mu.Lock()
			//候选人负责调用用来征集选票（5.2 节）
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
				LastLogTerm: savedLastLogTerm,//候选人最后日志条目的任期号
				LastLogIndex: savedLastLogIndex,//候选人的最后日志条目的索引值
			}
			var reply RequestVoteReply
			cm.debugLog("发送 RequestVote to %d: %+v", peerId, args)
			// 发起一个RPC Call
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				// 统计活的的投票个数
				cm.mu.Lock() //阻塞
				defer cm.mu.Unlock()
				cm.debugLog("接收 RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					// 在RPC期间 自己的角色已经发生了改变
					return //直接退出
				}

				if reply.Term > savedCurrentTerm {
					cm.debugLog("接收到一个任期大于自己的投票,直接变为Follower %+v", reply)
					// 如果回复的任期大于自己的任期，表示有新的已经开始了，新的一轮，一次自己直接变成跟随者
					cm.becomeFollower(reply.Term)
					// 这里有一个隐藏
					// 当接收到别自己到的任期后，变成跟随者时，上一个任期的选举计时器也会失效
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						// 任期是自己所在的任期，且有投票给自己
						votes := atomic.AddUint32(&votesCount, 1) //个数加一
						if votes*2 > uint32(len(cm.peerIds))+1 {
							// 获取大多数的投票
							// 变成领导者，选举计时器会退出
							cm.debugLog("id %d wins election with %d votes", cm.id, votes)
							cm.becomeLeader()
							// 这里变成领导者后，直接退出，不在进行，下面的选举计时器
							return
						}
						cm.debugLog("获得的投票数 %v", votes)
					}
				}
			}
		}(peerId)
	}
	// 开始一个新的选举计时器，上一个计时器会退出
	go cm.runElectionTimer()
}

// 变成追随者
func (cm *ConsensusModule) becomeFollower(term int) {
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
	// 当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的 index
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = None
	}
	go func() {
		// 心跳计时器
		ticker := time.NewTicker(cm.HeartbeatTimeDuration)
		defer ticker.Stop()

		for {
			// 发送心跳信息
			cm.leaderSendHeartbeats()
			<-ticker.C
			cm.mu.Lock()
			// 心跳信息只能由领导者发出,因此,每一轮心跳都要检查自己的身份
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
// 论文中 心跳信息跟日志追加相同
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {

		go func(peerId int) {
			// 论文中说明，如果日志追加是空，则表示为心跳信息
			cm.mu.Lock()
			nextIndex := cm.nextIndex[peerId]
			// 刚获得Leader时 自身logIndex + 1
			prevLogIndex := nextIndex - 1 //紧邻新日志条目之前的那个日志条目的索引
			prevLogTerm := None           //紧邻新日志条目之前的那个日志条目的任期
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			// 将没有同步newLogEntries日志,同步给Follower
			newLogEntries := cm.log[nextIndex:]
			// 当newLogEntries 为空时,为心跳信息

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				Entries:      newLogEntries,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: cm.commitIndex, //领导者的已知已提交的最高的日志条目的索引
			}
			cm.mu.Unlock()
			cm.debugLog("发送 AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("收到来自Follower的任期 %d", reply.Term)
				if reply.Term > savedCurrentTerm {
					// 心跳信息中的任期大于当前任期,则变为跟随者
					cm.debugLog("RPC reply 消息中的Term 大于自己的Term ,自身由 %s ---> Follower", cm.state.String())
					cm.becomeFollower(reply.Term)
					// 同时 心跳计时器退出
					return
				}

				//在被跟随者拒绝之后，领导人就会减小 nextIndex 值并进行重试。最终 nextIndex 会在某个位置使得领导人和跟随者的日志达成一致。
				//当这种情况发生，附加日志 RPC 就会成功，这时就会把跟随者冲突的日志条目全部删除并且加上领导人的日志。
				//一旦附加日志 RPC 成功，那么跟随者的日志就会和领导人保持一致，并且在接下来的任期里一直继续保持。
				if cm.state == Leader && savedCurrentTerm == reply.Term {
					// savedCurrentTerm == reply.Term  确保回复的当前任期的请求
					if reply.Success {
						// 对于每一台服务器，发送到该服务器的下一个日志条目的索引
						cm.nextIndex[peerId] = nextIndex + len(newLogEntries)
						// matchIndex 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						// 统计相应成功的个数,如果日志被大多数应用,这Leader 更新commitIndex ,表示这一条日志被永久应用到状态机
						savedCommitIndex := cm.commitIndex

						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							// 从上一此应用到日志的索引开始统计
							// 5.4.2 如果一个新的领导人要重新复制之前的任期里的日志时，它必须使用当前新的任期号
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1 // 自己有一票
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								// 如果 第 i 条日志 ,被大多数matchCount*2>len(cm.peerIds)+1 节点commit ,则 该 日志被应用
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex > savedCommitIndex {
							// 表示有新的日志被应用,则通知客户端
							cm.debugLog("Leader 有新的日志被应用到状态机.. commitIndex %d", cm.commitIndex)
							cm.debugLog(" Leader all log %v", cm.log)
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						// 被跟随者拒绝之后，领导人就会减小 nextIndex 值并进行重试。最终 nextIndex 会在某个位置使得领导人和跟随者的日志达成一致。
						cm.nextIndex[peerId] = nextIndex - 1
						cm.debugLog(" 日志被跟随者拒绝, nextIndex 向前移动 nextIndex %d --> %d ", nextIndex, nextIndex-1)
					}
				} else {
					cm.debugLog("在 AppendEntries RPC 期间,角色已经发生了变化....")
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
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.debugLog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.voteFor)
	// 开始投票
	if args.Term > cm.currentTerm {
		// 论文实现
		// 收到的投票请求中的任期大于自己的任期，自己变为追随者
		//论文5.4.1 除了比较任期,还需要比较日志大小
		cm.debugLog("收到的投票请求中的任期大于自己的任期，自己变为追随者")
		cm.becomeFollower(args.Term)
	}
	// 论文5.4.1 选举限制
	//除了比较任期,还需要比较日志大小
	// 能够投票的判断依据
	// 请求的任期大于自己的任期
	// 需要比较日志大小
	canVote := args.Term == cm.currentTerm &&
		(cm.voteFor == None || cm.voteFor == args.CandidateId) &&
		((args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) ||
		args.LastLogTerm > lastLogTerm)
	// (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) 索引是可以相同的,表示两边具有相同的日志
	// 论文实现
	if canVote {
		// 可以正常投票
		cm.debugLog("可以 %d 投票",args.CandidateId)
		cm.voteFor = args.CandidateId // 投票
		cm.electionTime = time.Now()  // 更新选举计时器
		reply.VoteGranted = true      // 投票
	} else {
		cm.debugLog("不能对 %d 投票 ",args.CandidateId)
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
		/*
		      接收者的实现：
		   	返回假 如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1 节）
		      返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在preLogIndex上能和prevLogTerm匹配上 （译者注：在接收者日志中 如果能找到一个和preLogIndex以及prevLogTerm一样的索引和任期的日志条目 则返回真 否则返回假）（5.3 节）
		      如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
		      追加日志中尚未存在的任何新条目
		      如果领导者的已知已经提交的最高的日志条目的索引 大于 接收者的已知已经提交的最高的日志条目的索引
		   则把 接收者的已知已经提交的最高的日志条目的索引 重置为 领导者的已知已经提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值

		*/

		// 日志的一致性检查
		// raft 保持两个原则
		//如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们存储了相同的指令。
		//如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们之前的所有日志条目也全部相同。
		if args.PrevLogIndex == None || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {

			// 寻找到一个和preLogIndex以及prevLogTerm一样的索引和任期的日志条目

			matchInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				// 如果matchInsertIndex>=len(cm.log) 表示,log最新一条日志跟Leader 的 PrevLogIndex保持一致
				// 如果 newEntriesIndex>len(args.Entries) 表示 没有找到一条跟Leader 的PrevLogIndex保持一致
				if matchInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				// cm.log[matchInsertIndex].Term != args.Entries[newEntriesIndex].Term
				// 表示一致性检查
				//跟随者冲突的日志条目全部删除并且加上领导人的日志
				if cm.log[matchInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				matchInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				// 表示至少还有一条日志是 Leader 跟Follower 是不一致的,此时追加日志,删除后面Follower后面不一致的日志
				cm.log = append(cm.log[:matchInsertIndex], args.Entries[newEntriesIndex:]...)
			}
			// 如果领导者的已知已经提交的最高的日志条目的索引 大于 接收者的已知已经提交的最高的日志条目的索引
			//则把 接收者的已知已经提交的最高的日志条目的索引 重置为 领导者的已知已经提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = logMin(args.LeaderCommit, len(cm.log)-1)
				cm.debugLog("Follower 应用新的日志到状态机 commitIndex %d", cm.commitIndex)
				cm.debugLog("Follower all log %v", cm.log)
				cm.newCommitReadyChan <- struct{}{}
			}

			reply.Success = true // 更新成功
		}

	}
	reply.Term = cm.currentTerm
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}

// lastLogIndexAndTerm 获取最新的日志索引和任期
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		lastTerm := cm.log[lastIndex].Term
		return lastIndex, lastTerm
	}
	return -1, -1
}

// 提交命令到log
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.debugLog("Submit received command ... %s %d ", cm.state.String(), command)
	if cm.state == Leader {
		// 日志只能在Leader提交
		cm.log = append(cm.log, LogEntry{
			Command: command,
			Term:    cm.currentTerm,
		})
		cm.debugLog("Leader all log... %v ", cm.log)
		return true
	}
	return false
}

// commitChanSender 发送已经应用的消息到客户端
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// 接收消息准备好的信号,将已经应用的日志发送给客户端
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied

		var entries []LogEntry

		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.debugLog("commitChanSender entries=%v savedLastApplied=%v", entries, savedLastApplied)
		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
}

func logMin(a, b int) int {
	if a > b {
		return b
	}
	return a
}
