package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type PutBody struct {
	Key      string `json:"key"`
	NewValue string `json:"value"`
	LogIndex uint64 `json:"log_index"`
}

type PutResponse struct {
	LogIndex uint64 `json:"log_index"`
}

func Put(state *State, replication_trigger *ReplicationTrigger, w http.ResponseWriter, r *http.Request) {
	var body PutBody
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := PutResponse{}

	state.mutex.Lock()

	index, err := FindLastValueIndex(state.pers_state.GetCommitLength(), state.log, []byte(body.Key))
	if err != nil {
		state.mutex.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if index != int64(body.LogIndex) {
		state.mutex.Unlock()
		http.Error(w, "Object has changed", http.StatusBadRequest)
		return
	}

	if state.node_id != state.current_leader {
		http.Error(w, "Follower can not serve put request", http.StatusBadRequest)
		state.mutex.Unlock()
		return
	}

	key_bytes := []byte(body.Key)
	value_bytes := []byte(body.NewValue)

	state.log.Append(Entry{term: state.pers_state.GetTerm(), msg: Serialize(key_bytes, value_bytes)})
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
