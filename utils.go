package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func ReadUint32(file *os.File) (uint32, error) {
	bytes := make([]byte, 4)
	_, err := io.ReadFull(file, bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytes), nil
}

func ReadUint64(file *os.File) (uint64, error) {
	bytes := make([]byte, 8)
	_, err := io.ReadFull(file, bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(bytes), nil
}

func WriteAtUint64(file *os.File, value uint64, off int64) error {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	_, err := file.WriteAt(bytes, off)
	return err
}

func GetNodeId(c context.Context) (uint64, error) {
	md, ok := metadata.FromIncomingContext(c)
	if !ok {
		err := status.Error(codes.NotFound, "node_id was not found")
		return 0, err
	}
	client_node_str := md.Get("node_id")
	if len(client_node_str) != 1 {
		err := status.Error(codes.NotFound, "node_id was not found")
		return 0, err
	}
	node_id, err := strconv.ParseUint(client_node_str[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return node_id, nil
}

func ParseKey(msg []byte) ([]byte, error) {
	key_len := binary.LittleEndian.Uint32(msg[:4])
	if len(msg) < int(key_len)+4 {
		return []byte{}, io.ErrUnexpectedEOF
	}
	return msg[4 : key_len+4], nil
}

func ParseValue(msg []byte, key []byte) ([]byte /* is_deleted */, bool, error) {
	if msg[4+len(key)] != 0 {
		return []byte{}, true, nil
	}
	return msg[len(key)+5:], false, nil
}

func Serialize(key []byte, value []byte) []byte {
	bytes := make([]byte, 4+len(key)+1+len(value))
	binary.LittleEndian.PutUint32(bytes, uint32(len(key)))
	copy(bytes[4:], key)
	is_deleted := []byte{0}
	copy(bytes[4+len(key):5+len(key)], is_deleted)
	copy(bytes[5+len(key):], value)
	return bytes
}

func SerializeErase(key []byte) []byte {
	bytes := make([]byte, 4+len(key)+1)
	binary.LittleEndian.PutUint32(bytes, uint32(len(key)))
	copy(bytes[4:], key)
	is_deleted := []byte{1}
	copy(bytes[4+len(key):], is_deleted)
	return bytes
}

func FindLastValueIndex(commit_length uint64, log *Log, key []byte) (int64, error) {
	for i := commit_length; i >= 1; i-- {
		msg := log.GetEntry(uint32(i - 1)).msg
		curr_key, err := ParseKey(msg)
		if err != nil {
			return -1, err
		}
		if !bytes.Equal(curr_key, key) {
			continue
		}
		return int64(i - 1), nil
	}
	return -1, nil
}
