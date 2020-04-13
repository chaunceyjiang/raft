package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// 实现论文中的两种RPC

type Server struct {
	mu sync.Mutex

	serverId uint64
	peerIds  []uint64

	cm *ConsensusModule

	// rpc 服务
	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[uint64]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}

	wg sync.WaitGroup
}

// 开始服务
func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready)
}

func (s *Server) Call(id uint64, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peerClient := s.peerClients[id]
	s.mu.Unlock()

	if peerClient != nil {
		return peerClient.Call(serviceMethod, args, reply)
	} else {
		return fmt.Errorf("call client %d after it's closed", id)
	}
}

// 论文实现
type RequestVoteArgs struct {
	Term         uint64 // 候选者的任期
	CandidateId  uint64 // 候选者的id
	LastLogIndex uint64 // 日志中 最新的index
	LastLogTerm  uint64 // 日志中最新的任期
}

type RequestVoteReply struct {
	Term        uint64 //自己的任期
	VoteGranted bool // 是否投票
}

// 论文实现
type AppendEntriesArgs struct {
	Term     uint64
	LeaderId uint64
	// 每次心跳会带上试探信息
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}
