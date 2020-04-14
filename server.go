package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 实现论文中的两种RPC

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	Cm *ConsensusModule

	// rpc 服务
	rpcServer *rpc.Server
	listener  net.Listener
	rpcProxy  *RPCProxy

	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}

	wg sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready <-chan interface{}) *Server {
	return &Server{
		mu:          sync.Mutex{},
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		ready:       ready,
		quit:        make(chan interface{}),
		wg:          sync.WaitGroup{},
	}
}

// 开始服务
func (s *Server) Serve() {
	s.mu.Lock()
	s.Cm = NewConsensusModule(s.serverId, s.peerIds, s, s.ready)

	// 开启一个RPC服务
	s.rpcServer = rpc.NewServer()

	s.rpcProxy = &RPCProxy{cm: s.Cm}

	s.rpcServer.RegisterName("ConsensusModule",s.rpcProxy)
	var err error

	s.listener,err = net.Listen("tcp",":0")
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())

	s.mu.Unlock()


	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		for {
			conn,err:=s.listener.Accept()
			if err!=nil{
				select {
				case <- s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}

			s.wg.Add(1)
			go func() {
				// 处理RPC请求
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peerClient := s.peerClients[id]
	s.mu.Unlock()

	if peerClient != nil {
		return peerClient.Call(serviceMethod, args, reply)
	} else {
		return fmt.Errorf("call client %d after it's closed", id)
	}
}


// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
func (s *Server) Shutdown() {
	s.Cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}



// RPC 代理ConsensusModule ，这代理会代理 两个RPC方法
type RPCProxy struct {
	cm *ConsensusModule
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		// 测试
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.debugLog("drop RequestVote")
			// 放弃这个RPC请求
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.debugLog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
			// 延迟这个RPC
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}


func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.debugLog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.debugLog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

// 论文实现
type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateId  int // 候选者的id
	LastLogIndex int // 日志中 最新的index
	LastLogTerm  int // 日志中最新的任期
}

type RequestVoteReply struct {
	Term        int //自己的任期
	VoteGranted bool   // 是否投票
}

// 论文实现
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// 每次心跳会带上试探信息
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
