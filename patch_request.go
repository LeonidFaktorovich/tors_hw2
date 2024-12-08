package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type PatchBody struct {
	Key    string `json:"key"`
	OpCode string `json:"op_code"`
	Params string `json:"params"`
}

type PatchResponse struct {
	Value    string `json:"value"`
	LogIndex uint64 `json:"log_index"`
}

func ApplyOperation(value []byte, op_code string, params string) ([]byte, error) {
	if op_code == "concat" {
		params_bytes := []byte(params)
		return append(value, params_bytes...), nil
	}
	return []byte{}, errors.New(fmt.Sprintf("Unknown operator `%v`", params))
}

func Patch(state *State, replication_trigger *ReplicationTrigger, w http.ResponseWriter, r *http.Request) {
	var body PatchBody
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := PatchResponse{}

	state.mutex.Lock()

	if state.node_id != state.current_leader {
		http.Error(w, "Follower can not serve patch request", http.StatusBadRequest)
		state.mutex.Unlock()
		return
	}

	index, err := FindLastValueIndex(state.pers_state.GetCommitLength(), state.log, []byte(body.Key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		state.mutex.Unlock()
		return
	}
	if index == -1 {
		http.Error(w, "not found", http.StatusNotFound)
		state.mutex.Unlock()
		return
	}

	value, is_deleted, err := ParseValue(state.log.GetEntry(uint32(index)).msg, []byte(body.Key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		state.mutex.Unlock()
		return
	}
	if is_deleted {
		http.Error(w, "not found", http.StatusNotFound)
		state.mutex.Unlock()
		return
	}

	key_bytes := []byte(body.Key)
	value_bytes, err := ApplyOperation(value, body.OpCode, body.Params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		state.mutex.Unlock()
		return
	}
	response.Value = string(value_bytes)
	state.log.Append(Entry{term: state.pers_state.GetTerm(), msg: Serialize(key_bytes, value_bytes)})
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
