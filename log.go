package main

import (
	"encoding/binary"
	"io"
	"os"
)

type Entry struct {
	term uint64
	msg  []byte
}

func (e Entry) Bytes() []byte {
	bytes := make([]byte, 4+8+len(e.msg))
	binary.LittleEndian.PutUint32(bytes, uint32(len(e.msg)))
	binary.LittleEndian.PutUint64(bytes[4:], e.term)

	copy(bytes[12:], e.msg)
	return bytes
}

func ReadEntrySize(f *os.File) (uint32, error) {
	value, err := ReadUint32(f)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func ReadEntryValue(f *os.File, size uint32) (Entry, error) {
	term, err := ReadUint64(f)
	if err != nil {
		return Entry{}, err
	}

	msg_bytes := make([]byte, size)
	_, err = io.ReadFull(f, msg_bytes)
	if err != nil {
		return Entry{}, err
	}
	return Entry{term: term, msg: msg_bytes}, nil
}

func ReadEntry(f *os.File) (Entry, error) {
	size, err := ReadEntrySize(f)
	if err != nil {
		return Entry{}, err
	}

	entry, err := ReadEntryValue(f, size)
	if err != nil {
		return Entry{}, err
	}
	return entry, nil
}

type Log struct {
	file    *os.File
	entries []Entry
}

func CreateLog(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	entries := []Entry{}
	for {
		entry, err := ReadEntry(file)
		if err == nil {
			entries = append(entries, entry)
		} else {
			curr_pos, seek_err := file.Seek(0, io.SeekCurrent)
			if seek_err != nil {
				panic(seek_err)
			}
			trunc_err := file.Truncate(curr_pos)
			if trunc_err != nil {
				panic(trunc_err)
			}
			break
		}
	}

	return &Log{file: file, entries: entries}, nil
}

func (log *Log) Append(entry Entry) error {
	bytes := entry.Bytes()
	n, err := log.file.Write(bytes)
	if n != len(bytes) {
		return io.ErrShortWrite
	}
	if err != nil {
		return err
	}

	log.entries = append(log.entries, entry)
	return nil
}

func (log *Log) GetEntry(i uint32) Entry {
	return log.entries[i]
}

func (log *Log) GetSize() uint32 {
	return uint32(len(log.entries))
}

func (log *Log) Truncate(i uint32) error {
	size := i * 12
	for entry_index := uint32(0); entry_index < i; entry_index++ {
		size += uint32(len(log.GetEntry(entry_index).msg))
	}
	err := log.file.Truncate(int64(size))
	if err != nil {
		return err
	}
	log.entries = log.entries[:i]
	return nil
}
