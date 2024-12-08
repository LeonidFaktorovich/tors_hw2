package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type DeleteBody struct {
	Key string `json:"key"`
}

type DeleteResponse struct {
	LogIndex uint64 `json:"log_index"`
}

func Delete(state *State, replication_trigger *ReplicationTrigger, w http.ResponseWriter, r *http.Request) {
	var body DeleteBody
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := PostResponse{}

	state.mutex.Lock()

	if state.node_id != state.current_leader {
		http.Error(w, "Follower can not serve delete request", http.StatusBadRequest)
		state.mutex.Unlock()
		return
	}

	key_bytes := []byte(body.Key)
	state.log.Append(Entry{term: state.pers_state.GetTerm(), msg: SerializeErase(key_bytes)})
	state.acked_length[state.node_id] = state.log.GetSize()
	log_index := state.log.GetSize() - 1

	replication_trigger.TriggerAll()

	for uint64(log_index) >= state.pers_state.GetCommitLength() {
		fmt.Printf("Log index is `%v`, current commit length: `%v`\n", log_index, state.pers_state.GetCommitLength())
		state.commit_cv.Wait()
	}
	state.mutex.Unlock()
	response.LogIndex = uint64(log_index)

	w.Header().Set("Content-Type", "application/json")
	jsonResp, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(jsonResp)
}