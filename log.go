package raft

// 日志
type LogEntry struct {
	Command interface{}
	Term    int
}
