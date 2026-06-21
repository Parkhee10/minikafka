package broker

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	pb "github.com/parkheejha10/minikafka/proto"
)

// Server implements the gRPC Broker service. It owns all topics and
// consumer groups for this broker process, and translates incoming
// RPC calls into calls on our existing Topic/Partition/ConsumerGroup
// types — none of the core engine had to change to support the
// network layer, which is exactly the point of having a clean
// internal API in the first place.
type Server struct {
	pb.UnimplementedBrokerServer

	dataDir string

	mu     sync.Mutex
	topics map[string]*Topic
	groups map[string]*ConsumerGroup
}

func NewServer(dataDir string) *Server {
	return &Server{
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
		groups:  make(map[string]*ConsumerGroup),
	}
}

func (s *Server) getOrCreateTopic(name string) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t, ok := s.topics[name]; ok {
		return t, nil
	}

	const defaultPartitions = 3
	t := &Topic{Name: name, partitions: make([]*Partition, defaultPartitions)}
	for i := int32(0); i < defaultPartitions; i++ {
		walPath := filepath.Join(s.dataDir, fmt.Sprintf("%s-%d.wal", name, i))
		p, err := NewPersistentPartition(name, i, walPath)
		if err != nil {
			return nil, fmt.Errorf("creating partition %d for topic %s: %w", i, name, err)
		}
		t.partitions[i] = p
	}
	s.topics[name] = t
	return t, nil
}

func (s *Server) getOrCreateGroup(name string) *ConsumerGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, ok := s.groups[name]; ok {
		return g
	}
	g := NewConsumerGroup(name)
	s.groups[name] = g
	return g
}

func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	topic, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		return nil, err
	}
	partitionID, offset, err := topic.Produce(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &pb.ProduceResponse{
		Partition: partitionID,
		Offset:    offset,
	}, nil
}

func (s *Server) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	topic, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		return nil, err
	}
	partition := topic.Partition(req.Partition)
	if partition == nil {
		return nil, fmt.Errorf("partition %d does not exist for topic %s", req.Partition, req.Topic)
	}

	maxMessages := int(req.MaxMessages)
	if maxMessages <= 0 {
		maxMessages = 100
	}
	messages := partition.Fetch(req.Offset, maxMessages)

	resp := &pb.FetchResponse{Messages: make([]*pb.MessageProto, 0, len(messages))}
	for _, m := range messages {
		resp.Messages = append(resp.Messages, &pb.MessageProto{
			Key:       m.Key,
			Value:     m.Value,
			Offset:    m.Offset,
			Timestamp: m.Timestamp.UnixNano(),
		})
	}
	return resp, nil
}

func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	group := s.getOrCreateGroup(req.Group)
	group.Commit(req.Topic, req.Partition, req.Offset)
	return &pb.CommitResponse{Success: true}, nil
}

// JoinGroup is a minimal placeholder for now: assigns ALL partitions
// to the joining consumer (no real rebalancing yet — that's Day 7).
func (s *Server) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	topic, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		return nil, err
	}
	assigned := make([]int32, topic.NumPartitions())
	for i := range assigned {
		assigned[i] = int32(i)
	}
	return &pb.JoinGroupResponse{AssignedPartitions: assigned}, nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.topics {
		t.Close()
	}
}
