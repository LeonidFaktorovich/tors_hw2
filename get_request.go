package main

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type GetBody struct {
	Key string `json:"key"`
}

type GetResponse struct {
	Value    string `json:"value"`
	LogIndex uint64 `json:"log_index"`
}

func Get(state *State, w http.ResponseWriter, r *http.Request) {
	var body GetBody
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := GetResponse{}

	state.mutex.RLock()
	defer state.mutex.RUnlock()

	commit_length := state.pers_state.GetCommitLength()
	not_found := true
	for i := commit_length; i >= 1; i-- {
		msg := state.log.GetEntry(uint32(i - 1)).msg
		key, err := ParseKey(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !bytes.Equal(key, []byte(body.Key)) {
			continue
		}
		value, err := ParseValue(msg, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response.Value = string(value)
		response.LogIndex = i - 1
		not_found = false
		break
	}
	if not_found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsonResp, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(jsonResp)
}
