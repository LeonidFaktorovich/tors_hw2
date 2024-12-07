package main

import (
	"fmt"
	"log"
	"net"
	"raft/internal_service"

	"google.golang.org/grpc"
)

func GrpcServiceDaemon(port int, state *State) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	internal_service.RegisterInternalServiceServer(s, &InternalService{state: state})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
