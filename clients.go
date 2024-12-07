package main

import (
	"fmt"
	"raft/internal_service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Clients struct {
	conns map[uint64]*internal_service.InternalServiceClient
}

func CreateClients(addrs map[uint64]Addr) (*Clients, error) {
	conns := make(map[uint64]*internal_service.InternalServiceClient)
	for node_id, addr := range addrs {
		conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", addr.domain, addr.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		service_conn := internal_service.NewInternalServiceClient(conn)
		conns[node_id] = &service_conn
	}
	return &Clients{conns: conns}, nil
}
