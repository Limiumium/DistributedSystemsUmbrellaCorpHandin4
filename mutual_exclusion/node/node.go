package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
	logger         *slog.Logger
}

// This is the function for Log, should be at start before main(), do not move
func initLogger(nodeID uint64) (*slog.Logger, *os.File, error) {
	//This here creates Logs directory if it doesnt exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, nil, err
	}

	//Create a separate Log file for each node in here
	filename := filepath.Join(
		"logs",
		fmt.Sprintf("node-%d.log", nodeID),
	)

	//Create or append to the nodes Log file, in here
	file, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, nil, err
	}

	//Write logs to both terminal and file
	writer := io.MultiWriter(os.Stdout, file)

	handler := slog.NewTextHandler(
		writer,
		&slog.HandlerOptions{
			Level: slog.LevelInfo,
		},
	)

	logger := slog.New(handler)

	return logger, file, nil

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

	//Initialize logger for this node
	logger, logFile, err := initLogger(server.id)

	if err != nil {
		fmt.Println("Could not initialize logger:", err)
		return
	}

	//Close log file when the server is shut down
	defer logFile.Close()

	//Store logger in the Server struct
	server.logger = logger

	//initialize node state and maps
	initialize(server)

	server.logger.Info(
		"Node started",
		"node_id", server.id,
		"address", listener.Addr().String(),
	)

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

		server.logger.Info(
			"NODE_SHUTTING_DOWN",
			"node_id", server.id,
		)

		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(listener); err != nil {
		server.logger.Error(
			"GRPC_SERVER_ERROR",
			"error", err,
		)

		server.logger.Info(
			"NODE_STOPPED",
			"node_id", server.id,
		)
	}

}

func initialize(s *Server) {
	s.requestStreams = make(map[uint64]pb.ChatService_ChatStreamServer)
	s.grantStreams = make(map[uint64]pb.ChatService_ChatStreamServer)
	s.queue = nil
	s.state = "RELEASED"

	s.logger.Info(
		"NODE_IS_INITIALIZED",
		"node_id", s.id,
		"state", s.state,
	)
}

func enter(s *Server) {
	s.state = "WANTED"

	s.logger.Info(
		"REQUESTING_CRITICAL_SECTION",
		"node_id", s.id,
		"state", s.state,
	)

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
		s.logger.Error(
			"MESSAGE_RECEIVE_FAILED",
			"node_id", s.id,
			"peer_id", id,
			"error", err,
		)
		return
	}

	s.logger.Info(
		"REQUEST_RECEIVED",
		"node_id", s.id,
		"peer_id", msg.Id,
		"timestamp", msg.Timestamp,
	)

	if s.state == "HELD" ||
		(s.state == "WANTED" && compare_priority(s, msg)) {

		s.queue = append(s.queue, s.grantStreams[msg.Id])

		s.logger.Info(
			"REPLY_DEFERRED",
			"node_id", s.id,
			"peer_id", msg.Id,
			"state", s.state,
		)
	} else {
		s.timeMu.Lock()
		s.lamportTime = max(s.lamportTime, msg.Timestamp) + 1
		s.timeMu.Unlock()
		reply := &pb.ChatMessage{
			Id:        s.id,
			Timestamp: s.lamportTime,
		}

		if err := s.grantStreams[msg.Id].Send(reply); err != nil {
			s.logger.Error(
				"REPLY_SEND_FAILED",
				"node_id", s.id,
				"peer_id", msg.Id,
				"error", err,
			)
			return
		}

		s.logger.Info(
			"REPLY_SENT",
			"node_id", s.id,
			"peer_id", msg.Id,
			"timestamp", reply.Timestamp,
		)
	}
}

func exit(s *Server) {

	s.state = "RELEASED"

	s.logger.Info(
		"EXIT_CRITICAL_SECTION",
		"node_id", s.id,
		"state", s.state,
	)

	reply := &pb.ChatMessage{
		Id:        s.id,
		Timestamp: s.lamportTime,
	}
	for _, clientStream := range s.queue {

		if err := clientStream.Send(reply); err != nil {

			s.logger.Error(
				"DEFERRED_REPLY_FAILED",
				"node_id", s.id,
				"error", err,
			)

			continue
		}

		s.logger.Info(
			"DEFERRED_REPLY_SENT",
			"node_id", s.id,
		)

		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	//Empty the queue
	s.queue = nil
}

func access_cs(s *Server) {
	s.logger.Info(
		"ENTER_CRITICAL_SECTION!",
		"node_id", s.id,
	)

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
