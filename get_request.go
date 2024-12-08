package main

import (
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

	index, err := FindLastValueIndex(state.pers_state.GetCommitLength(), state.log, []byte(body.Key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if index == -1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	value, err := ParseValue(state.log.GetEntry(uint32(index)).msg, []byte(body.Key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
