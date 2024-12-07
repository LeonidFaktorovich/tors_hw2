package main

import (
	"context"
	"fmt"
	"raft/internal_service"
	"time"
)

type InternalService struct {
	internal_service.UnimplementedInternalServiceServer
	state *State
}

func AppendEntries(state *State, prev_log_length uint32, leader_commit uint64, entries []*internal_service.Entry) {
	if len(entries) > 0 && state.log.GetSize() > prev_log_length {
		if state.log.GetEntry(prev_log_length).term != entries[0].Term {
			state.log.Truncate(prev_log_length)
		}
	}
	if prev_log_length+uint32(len(entries)) > state.log.GetSize() {
		for i := state.log.GetSize() - prev_log_length; i < uint32(len(entries)); i++ {
			entry := Entry{term: entries[i].Term, msg: entries[i].Msg}
			state.log.Append(entry)
		}
	}
	if leader_commit > state.pers_state.GetCommitLength() {
		state.pers_state.SetCommitLength(leader_commit)
		state.commit_cv.Broadcast()
	}
}

func (service *InternalService) ReceiveLog(c context.Context, request *internal_service.LogRequest) (*internal_service.LogResponse, error) {
	service.state.mutex.RLock()
	current_term := service.state.pers_state.GetTerm()
	role := service.state.role
	service.state.mutex.RUnlock()

	node_id, err := GetNodeId(c)

	if request.Term > current_term {
		service.state.mutex.Lock()

		service.state.pers_state.SetTerm(request.Term)
		service.state.pers_state.SetVotedFor(0)
		service.state.role = Follower
		if err != nil {
			service.state.mutex.Unlock()
			return nil, err
		}
		service.state.current_leader = node_id

		service.state.mutex.Unlock()
	} else if request.Term == current_term && role == Candidate {
		service.state.mutex.Lock()

		service.state.role = Follower
		service.state.current_leader = node_id

		service.state.mutex.Unlock()
	}

	service.state.mutex.RLock()
	current_term = service.state.pers_state.GetTerm()
	log_ok := (service.state.log.GetSize() >= request.LogLength) && (request.LogLength == 0 || request.PrevLogTerm == service.state.log.GetEntry(request.LogLength-1).term)
	service.state.mutex.RUnlock()

	if request.Term == current_term && log_ok {
		service.state.mutex.Lock()

		service.state.last_leader_msg_tp = time.Now()
		AppendEntries(service.state, request.LogLength, request.LeaderCommit, request.Entries)

		service.state.mutex.Unlock()

		ack := request.LogLength + uint32(len(request.Entries))

		response := internal_service.LogResponse{}
		response.Ack = ack
		response.Appened = true
		response.NodeId = service.state.node_id
		response.Term = request.Term

		return &response, nil
	}

	service.state.mutex.RLock()
	log_term := uint64(0)
	if service.state.log.GetSize() >= request.LogLength && request.LogLength > 0 {
		log_term = service.state.log.GetEntry(request.LogLength - 1).term
	}
	fmt.Printf("Not acked entries. Curren log length: %v, request log length %v. Request prev log term: %v, current prev log term: %v. Request term: %v, current term: %v\n", service.state.log.GetSize(), request.LogLength, request.PrevLogTerm, log_term, request.Term, service.state.pers_state.GetTerm())
	service.state.mutex.RUnlock()

	response := internal_service.LogResponse{}
	response.Ack = 0
	response.Appened = false
	response.NodeId = service.state.node_id
	response.Term = current_term

	return &response, nil
}

func (service *InternalService) Vote(c context.Context, request *internal_service.VoteRequest) (*internal_service.VoteResponse, error) {
	service.state.mutex.RLock()

	my_log_length := service.state.log.GetSize()
	my_log_term := uint64(0)
	if my_log_length > 0 {
		my_log_term = service.state.log.GetEntry(my_log_length - 1).term
	}
	current_term := service.state.pers_state.GetTerm()

	service.state.mutex.RUnlock()

	log_ok := (request.LogTerm > my_log_term || (request.LogTerm == my_log_term && request.LogLength >= my_log_length))
	if !log_ok {
		return &internal_service.VoteResponse{Term: current_term, Voted: false, NodeId: service.state.node_id}, nil
	}

	service.state.mutex.RLock()
	voted_for := service.state.pers_state.GetVotedFor()
	service.state.mutex.RUnlock()
	term_ok := (request.Term > current_term || (request.Term == current_term && (voted_for == service.state.node_id || voted_for == 0)))

	if term_ok {
		node_id, err := GetNodeId(c)
		if err != nil {
			return nil, err
		}

		service.state.mutex.RLock()
		service.state.pers_state.SetTerm(request.Term)
		service.state.role = Follower
		fmt.Printf("Current host has become follower\n")

		service.state.pers_state.SetVotedFor(node_id)

		service.state.mutex.RUnlock()
	}

	return &internal_service.VoteResponse{Term: request.Term, Voted: term_ok, NodeId: service.state.node_id}, nil
}
