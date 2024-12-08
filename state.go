package main

import (
	"sync"
	"time"
)

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

type State struct {
	pers_state *PersistentStorage
	log        *Log

	node_id        uint64          // immutable
	nodes_count    uint64          // immutable
	nodes_addr     map[uint64]Addr // immutable
	role           int
	current_leader uint64
	voted_received map[uint64]struct{}
	sent_length    map[uint64]uint32
	acked_length   map[uint64]uint32

	last_leader_msg_tp time.Time
	leader_term        uint64

	mutex sync.RWMutex

	commit_cv sync.Cond
}

func CreateState(pers_storage_path string, log_path string, node_id uint64, nodes_count uint64, nodes_addr map[uint64]Addr) (*State, error) {
	pers_state, err := CreatePersistentStorage(pers_storage_path)
	if err != nil {
		return nil, err
	}

	log, err := CreateLog(log_path)
	if err != nil {
		return nil, err
	}

	sent_length := make(map[uint64]uint32)
	acked_length := make(map[uint64]uint32)
	for i := uint64(1); i <= nodes_count; i++ {
		sent_length[i] = 0
		acked_length[i] = 0
	}

	state := State{pers_state: pers_state, log: log, node_id: node_id, nodes_count: nodes_count, nodes_addr: nodes_addr, role: Follower, current_leader: 0, voted_received: make(map[uint64]struct{}), sent_length: sent_length, acked_length: acked_length, last_leader_msg_tp: time.Unix(0, 0), leader_term: 0}
	state.commit_cv = *sync.NewCond(&state.mutex)
	return &state, nil
}

func (s *State) GetRole() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.role
}

func (s *State) GetCurrentLeader() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.current_leader
}

func (s *State) GetLastLeaderMsgTp() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.last_leader_msg_tp
}
