package main

import (
	"context"
	"fmt"
	"raft/internal_service"
	"time"

	"google.golang.org/grpc/metadata"
)

func ProcessVote(response *internal_service.VoteResponse, state *State) int {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	fmt.Printf("Vote response from %v, term: %v, voted: %v. Current term: %v\n", response.NodeId, response.Term, response.Voted, state.pers_state.GetTerm())
	if state.role == Candidate && state.pers_state.GetTerm() == response.Term && response.Voted {
		fmt.Printf("Add vote from %v\n", response.NodeId)
		state.voted_received[response.NodeId] = struct{}{}
		if len(state.voted_received) >= (int(state.nodes_count)+1)/2 {
			state.current_leader = state.node_id
			state.role = Leader
			fmt.Printf("Current host has became leader\n")
			return Leader
		}
	} else if response.Term > state.pers_state.GetTerm() {
		fmt.Printf("Term is not corrent\n")
		state.pers_state.SetTerm(response.Term)
		state.role = Follower
		state.pers_state.SetVotedFor(0)
		return Follower
	}
	return state.role
}

func ElectionDaemon(state *State, clients *Clients, replication_trigger *ReplicationTrigger) {
	votes_channel := make(chan *internal_service.VoteResponse)

	if state.GetLastLeaderMsgTp() == time.Unix(0, 0) {
		state.mutex.Lock()
		state.last_leader_msg_tp = time.Now()
		state.mutex.Unlock()
	}

	for {
		if state.GetRole() == Leader {
			fmt.Println("Current host is leader")
			time.Sleep(5 * time.Second)
			continue
		}

		last_leader_msg_tp := state.GetLastLeaderMsgTp()
		if state.GetRole() == Follower && time.Since(last_leader_msg_tp) < 5*time.Second {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		fmt.Printf("Start leader election at %v\n", time.Now())

		state.mutex.Lock()

		next_term := state.pers_state.GetTerm() + 1
		state.pers_state.SetTerm(next_term)

		state.role = Candidate
		state.pers_state.SetVotedFor(state.node_id)
		state.voted_received[state.node_id] = struct{}{}
		log_term := uint64(0)
		log_lenth := state.log.GetSize()
		if log_lenth > 0 {
			log_term = state.log.GetEntry(log_lenth - 1).term
		}

		state.mutex.Unlock()

		req := internal_service.VoteRequest{}
		req.LogLength = log_lenth
		req.LogTerm = log_term
		req.Term = next_term

		md := metadata.Pairs("node_id", fmt.Sprintf("%v", state.node_id))

		for node_id, conn := range clients.conns {
			go func() {
				ctx := context.Background()
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
				defer cancel()
				ctx = metadata.NewOutgoingContext(ctx, md)
				fmt.Printf("Send vote request to %v\n", node_id)
				response, err := (*conn).Vote(ctx, &req)
				if err != nil {
					fmt.Printf("Error with vote request from %v: %v\n", node_id, err.Error())
					return
				}
				fmt.Printf("Receive vote response from %v, granted: %v\n", node_id, response.Voted)
				votes_channel <- response
			}()
		}

		timeout := time.After(5 * time.Second)

		for {
			should_exit := false
			select {
			case <-timeout:
				fmt.Printf("Election timeout\n")
				should_exit = true
			case vote := <-votes_channel:
				fmt.Printf("Process vote response from %v\n", vote.NodeId)
				role := ProcessVote(vote, state)
				if role == Follower {
					should_exit = true
					break
				}
				if role == Leader {
					state.mutex.Lock()
					log_length := state.log.GetSize()

					for i := uint64(1); i <= state.nodes_count; i++ {
						if i == state.node_id {
							continue
						}
						state.sent_length[i] = log_length
						state.acked_length[i] = 0
						fmt.Printf("Trigger replication to %v\n", i)
						replication_trigger.Trigger(i)
					}
					should_exit = true
					state.mutex.Unlock()
				}
			}
			if should_exit {
				break
			}
		}

	}
}
