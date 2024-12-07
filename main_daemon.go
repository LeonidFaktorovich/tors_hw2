package main

import "flag"

var node_id_flag = flag.Int("node_id", 0, "node id")

var http_port_flag = flag.Int("http_port", 0, "http server port")

var log_path_flag = flag.String("log_path", "", "log path")

var pers_storage_path_flag = flag.String("pers_storage_path", "", "persistent storage path")

type Addr struct {
	domain string
	port   int
}

func MainDaemon() {
	flag.Parse()

	nodes := make(map[uint64]Addr)
	nodes[1] = Addr{domain: "localhost", port: 10001}
	nodes[2] = Addr{domain: "localhost", port: 10002}
	nodes[3] = Addr{domain: "localhost", port: 10003}

	server_port := nodes[uint64(*node_id_flag)].port

	delete(nodes, uint64(*node_id_flag))

	clients, err := CreateClients(nodes)
	if err != nil {
		panic(err)
	}

	log_path := "/home/leonid-db/tors/hw_2/" + *log_path_flag
	pers_path := "/home/leonid-db/tors/hw_2/" + *pers_storage_path_flag

	state, err := CreateState(pers_path, log_path, uint64(*node_id_flag), 3, nodes)
	if err != nil {
		panic(err)
	}

	channels := make(map[uint64]chan struct{})
	for node_id := range nodes {
		channels[node_id] = make(chan struct{}, 1)
	}

	replication_trigger := ReplicationTrigger{channels: channels}

	go func() {
		ElectionDaemon(state, clients, &replication_trigger)
	}()

	go func() {
		HttpDaemon(state, &replication_trigger, *http_port_flag)
	}()

	StartReplicationDaemon(state, clients, &replication_trigger)

	GrpcServiceDaemon(server_port, state)
}
