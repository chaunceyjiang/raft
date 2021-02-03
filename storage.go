package raft

import "sync"

/*
raft 持久化:

currentTerm - the latest term this server has observed
votedFor - the peer ID for whom this server voted in the latest term
log - Raft log entries
*/

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	HasData() bool
}

// MapStorage 用于测试
type MapStorage struct {
	//m sync.Map
	mu sync.Mutex
	m map[string][]byte
}

func (m *MapStorage) Set(key string, value []byte) {
	//m.m.Store(key, value)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key]=value
}

func (m *MapStorage) Get(key string) ([]byte, bool) {
	//value, ok := m.m.Load(key)
	//return value.([]byte), ok
	m.mu.Lock()
	defer m.mu.Unlock()
	value,ok:=m.m[key]
	return value,ok
}

func (m *MapStorage) HasData() bool {
	//ok:=false
	//m.m.Range(func(key, value interface{}) bool {
	//	ok=true
	//	return false
	//})
	//return ok
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.m)>0
}

func NewMapStorage() *MapStorage {
	return &MapStorage{
		mu: sync.Mutex{},
		m:  make(map[string][]byte),
	}
}
