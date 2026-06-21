package main

import (
	"log"
	"net"
	"os"

	"github.com/parkheejha10/minikafka/broker"
	pb "github.com/parkheejha10/minikafka/proto"
	"google.golang.org/grpc"
)

func main() {
	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("creating data directory: %v", err)
	}

	srv := broker.NewServer(dataDir)
	defer srv.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServer(grpcServer, srv)

	const address = ":50051"
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", address, err)
	}

	log.Printf("MiniKafka broker listening on %s (data dir: %s)", address, dataDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
