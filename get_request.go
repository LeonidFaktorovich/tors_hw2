package main

import (
	"encoding/json"
	"net/http"
)

type GetResponse struct {
	Value    string `json:"value"`
	LogIndex uint64 `json:"log_index"`
}

func Get(state *State, w http.ResponseWriter, r *http.Request) {
	response := GetResponse{}

	state.mutex.RLock()
	defer state.mutex.RUnlock()

	key := r.URL.Query().Get("key")

	index, err := FindLastValueIndex(state.pers_state.GetCommitLength(), state.log, []byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if index == -1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	value, is_deleted, err := ParseValue(state.log.GetEntry(uint32(index)).msg, []byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if is_deleted {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	response.Value = string(value)
	response.LogIndex = uint64(index)

	w.Header().Set("Content-Type", "application/json")
	jsonResp, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(jsonResp)
}
