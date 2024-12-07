package main

import (
	"context"
	"fmt"
	"raft/internal_service"
	"sort"
	"time"

	"google.golang.org/grpc/metadata"
)

type ReplicationTrigger struct {
	channels map[uint64]chan struct{}
}

func (r *ReplicationTrigger) Trigger(node uint64) {
	select {
	case r.channels[node] <- struct{}{}:
	default:
	}
}

func (r *ReplicationTrigger) TriggerAll() {
	for _, channel := range r.channels {
		select {
		case channel <- struct{}{}:
		default:
		}
	}
}

func CommitLogEntries(state *State) {
	acked := make([]uint32, 0, state.nodes_count-1)

	for _, ack := range state.acked_length {
		acked = append(acked, ack)
	}

	sort.Slice(acked, func(i, j int) bool { return acked[i] < acked[j] })

	curr_commit_length := state.pers_state.GetCommitLength()
	new_commit_length := uint32(0)
	if uint64(len(acked)) > (state.nodes_count-1)/2 {
		new_commit_length = acked[(state.nodes_count-1)/2]
	}

	if new_commit_length > uint32(curr_commit_length) && state.log.GetEntry(new_commit_length-1).term == state.pers_state.GetTerm() {
		state.pers_state.SetCommitLength(uint64(new_commit_length))
		state.commit_cv.Broadcast()
	}
}

func StartReplicationDaemon(state *State, clients *Clients, replication_trigger *ReplicationTrigger) {
	for node_id, trigger := range replication_trigger.channels {
		go func() {
			for {
				<-trigger
				if state.GetRole() != Leader {
					continue
				}
				conn := clients.conns[node_id]
				ctx := context.Background()
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
				md := metadata.Pairs("node_id", fmt.Sprintf("%v", state.node_id))
				ctx = metadata.NewOutgoingContext(ctx, md)

				req := internal_service.LogRequest{}
				req.Entries = []*internal_service.Entry{}

				state.mutex.RLock()

				for i := state.sent_length[node_id]; i < state.log.GetSize(); i++ {
					proto_entry := internal_service.Entry{Term: state.log.GetEntry(i).term, Msg: state.log.GetEntry(i).msg}
					req.Entries = append(req.Entries, &proto_entry)
				}

				req.LeaderCommit = state.pers_state.GetCommitLength()
				req.LogLength = state.sent_length[node_id]
				req.PrevLogTerm = 0
				if state.sent_length[node_id] > 0 {
					req.PrevLogTerm = state.log.GetEntry(state.sent_length[node_id] - 1).term
				}
				req.Term = state.pers_state.GetTerm()

				state.mutex.RUnlock()

				response, err := (*conn).ReceiveLog(ctx, &req)
				cancel()
				if err != nil {
					go func() {
						time.Sleep(1 * time.Second)
						select {
						case trigger <- struct{}{}:
						default:
						}
					}()
					continue
				}

				state.mutex.RLock()

				current_term := state.pers_state.GetTerm()
				role := state.role
				acked := state.acked_length[node_id]
				sent_length := state.sent_length[node_id]

				state.mutex.RUnlock()

				if response.Term == current_term && role == Leader {
					if response.Appened && response.Ack >= acked {
						state.mutex.Lock()
						state.sent_length[node_id] = response.Ack
						state.acked_length[node_id] = response.Ack

						CommitLogEntries(state)

						state.mutex.Unlock()
					} else if sent_length > 0 {
						state.mutex.Lock()

						state.sent_length[node_id] = sent_length - 1

						state.mutex.Unlock()

						select {
						case trigger <- struct{}{}:
						default:
						}

						continue
					}
				} else if response.Term > current_term {
					state.mutex.Lock()

					state.pers_state.SetTerm(response.Term)
					state.role = Follower
					state.pers_state.SetVotedFor(0)

					state.mutex.Unlock()
				}

				go func() {
					time.Sleep(300 * time.Millisecond)
					select {
					case trigger <- struct{}{}:
					default:
					}
				}()
			}
		}()
	}

}
