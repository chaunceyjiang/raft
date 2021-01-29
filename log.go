package raft

// 日志
type LogEntry struct {
	Command interface{}
	Term    int
}


type CommitEntry struct {
	// 客户端提交的命令
	Command interface{}

	// 客户端提交的命令在log 中的索引
	Index int

	// 客户端提交的命令所在的任期
	Term int
}


