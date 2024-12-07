package main

import (
	"encoding/binary"
	"io"
	"os"
)

type PersistentStorage struct {
	file *os.File

	term          uint64
	voted_for     uint64
	commit_length uint64
}

func CreatePersistentStorage(path string) (*PersistentStorage, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		err = file.Truncate(8 * 3)
		if err != nil {
			return nil, err
		}
		var term uint64 = 0
		var voted_for uint64 = 0
		var commit_length uint64 = 0

		bytes_storage := make([]byte, 8)

		binary.LittleEndian.PutUint64(bytes_storage, term)
		_, err = file.WriteAt(bytes_storage, 0)
		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint64(bytes_storage, voted_for)
		_, err = file.WriteAt(bytes_storage, 8)
		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint64(bytes_storage, commit_length)
		_, err = file.WriteAt(bytes_storage, 16)
		if err != nil {
			return nil, err
		}

		return &PersistentStorage{file: file, term: term, voted_for: voted_for, commit_length: commit_length}, nil

	}
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	file.Seek(0, io.SeekStart)
	term, err := ReadUint64(file)
	if err != nil {
		return nil, err
	}

	file.Seek(8, io.SeekStart)
	voted_for, err := ReadUint64(file)
	if err != nil {
		return nil, err
	}

	file.Seek(16, io.SeekStart)
	commit_length, err := ReadUint64(file)
	if err != nil {
		return nil, err
	}

	return &PersistentStorage{file: file, term: term, voted_for: voted_for, commit_length: commit_length}, nil
}

func (s PersistentStorage) GetTerm() uint64 {
	return s.term
}

func (s *PersistentStorage) SetTerm(term uint64) error {
	err := WriteAtUint64(s.file, term, 0)
	if err != nil {
		return err
	}
	s.term = term
	return nil
}

func (s PersistentStorage) GetVotedFor() uint64 {
	return s.voted_for
}

func (s *PersistentStorage) SetVotedFor(voted_for uint64) error {
	err := WriteAtUint64(s.file, voted_for, 8)
	if err != nil {
		return err
	}
	s.voted_for = voted_for
	return nil
}

func (s PersistentStorage) GetCommitLength() uint64 {
	return s.commit_length
}

func (s *PersistentStorage) SetCommitLength(commit_length uint64) error {
	err := WriteAtUint64(s.file, commit_length, 16)
	if err != nil {
		return err
	}
	s.commit_length = commit_length
	return nil
}
