package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
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
	var MAX_NB_NODES = 10
	server := &Server{}
	var listener net.Listener
	var err error
	for i := 0; i < MAX_NB_NODES; i++ {
		address := "localhost:505" + strconv.Itoa(i)
		listener, err = net.Listen("tcp", address)
		if err != nil {
			fmt.Println(address + " is already in use")
			continue
		} else {
			server.id = uint64(i)
			fmt.Println("using: " + address)
			break
		}
	}

	if listener == nil {
		fmt.Println("All nodes are in use. Aborting...")
		return
	}

	grpcServer := grpc.NewServer()

	pb.RegisterChatServiceServer(grpcServer, server)

	//app termination listener
	closeChannel := make(chan os.Signal, 1)
	signal.Notify(
		closeChannel,
		os.Interrupt,    // Ctrl+C
		syscall.SIGTERM, // window close
	)

	go func() {
		<-closeChannel

		grpcServer.Stop()
	}()

	grpcServer.Serve(listener)
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
		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	//Waits for all processes to grant the request
	for _, clientStream := range s.grantStreams {
		clientStream.Recv()
		s.timeMu.Lock()
		s.lamportTime = max(s.lamportTime, msg.Timestamp) + 1
		s.timeMu.Unlock()
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
		s.timeMu.Lock()
		s.lamportTime = max(s.lamportTime, msg.Timestamp) + 1
		s.timeMu.Unlock()
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
		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	//Empty the queue
	s.queue = nil
}

func access_cs(s *Server) {
	//The process accesses the Critical Section (here it just prints to the terminal)
	fmt.Println("Process " + strconv.FormatUint(s.id, 10) + " accessed the Critical Section.")
	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()
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
