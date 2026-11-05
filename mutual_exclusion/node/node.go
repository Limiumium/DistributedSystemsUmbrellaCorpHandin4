package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	requestStreams map[uint64]pb.ChatService_ChatStreamServer //Channels to send requests to
	grantStreams   map[uint64]pb.ChatService_ChatStreamServer //Channels to grant requests to
	queue          []pb.ChatService_ChatStreamServer          //Queue of channels
	mu             sync.Mutex
	timeMu         sync.Mutex
	lamportTime    uint64
	state          string
	id             uint64
}

func main() {

}

func initialize(s *Server) {
	s.state = "RELEASED"
}

func enter(s *Server) {
	s.state = "WANTED"
	msg := &pb.ChatMessage{
		Id:        s.id,
		Timestamp: s.lamportTime,
	}

	//Sends a request to all other processes
	for _, clientStream := range s.requestStreams {
		clientStream.Send(msg)
	}

	//Waits for all processes to grant the request
	for _, clientStream := range s.grantStreams {
		clientStream.Recv()
	}

	//Access the Critical Section
	s.state = "HELD"
	access_cs(s)
}

func receive(s *Server, id uint64) {
	msg, err := s.grantStreams[id].Recv()
	if err != nil {
		panic(err)
	}
	if s.state == "HELD" ||
		(s.state == "WANTED" && compare_priority(s, msg)) {
		s.queue = append(s.queue, s.grantStreams[msg.Id])
	} else {
		reply := &pb.ChatMessage{
			Id:        s.id,
			Timestamp: s.lamportTime,
		}
		s.grantStreams[msg.Id].Send(reply)
	}
}

func exit(s *Server) {

	s.state = "RELEASED"
	reply := &pb.ChatMessage{
		Id:        s.id,
		Timestamp: s.lamportTime,
	}
	for _, clientStream := range s.queue {
		clientStream.Send(reply)
	}

	//Empty the queue

}

func access_cs(s *Server) {
	//The process accesses the Critical Section (here it just prints to the terminal)
	fmt.Println("Process " + strconv.FormatUint(s.id, 10) + " accessed the Critical Section.")
	time.Sleep(3 * time.Second)
	exit(s)
}

func compare_priority(s *Server, msg *pb.ChatMessage) bool {
	if s.lamportTime < msg.Timestamp {
		return true
	}
	if s.lamportTime > msg.Timestamp {
		return false
	}
	return s.id < msg.Id
}
