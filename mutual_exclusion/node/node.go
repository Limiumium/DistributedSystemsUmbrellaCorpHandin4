package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	pb "mutual_exclusion/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// interface so we can bundle servers and clients together in the same slice
type ChatStreamAll interface {
	Send(*pb.ChatMessage) error
	Recv() (*pb.ChatMessage, error)
}

type Server struct {
	pb.UnimplementedChatServiceServer

	allStreams        map[uint64]ChatStreamAll
	queue             []ChatStreamAll //Queue of channels
	mu                sync.Mutex
	timeMu            sync.Mutex
	lamportTime       uint64
	state             string
	id                uint64
	outstandingGrants map[uint64]struct{}
	grantWaitGroup    sync.WaitGroup
	logger            *slog.Logger
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
	server.allStreams = make(map[uint64]ChatStreamAll)

	//find what port (and id) we can use
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		//fmt.Println("checking: " + address)
		tmpListener, err := net.Listen("tcp", address)
		if err != nil {
			fmt.Println(address + " is already in use")

		} else {
			server.id = uint64(i)
			fmt.Println("using: " + address)
			tmpListener.Close()
			break
		}
	}

	//create clients for existing servers and set the listener for our own server
	var listener net.Listener
	var processes uint64
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		//fmt.Println("checking: " + address)
		tmpListener, err := net.Listen("tcp", address)
		if err != nil {
			conn, _ := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			defer conn.Close()

			client := pb.NewChatServiceClient(conn)

			//create metaData
			md := metadata.Pairs(
				"id", strconv.FormatUint(server.id, 10),
			)
			ctx := metadata.NewOutgoingContext(context.Background(), md)

			//connect to chatStream
			chatStream, err := client.ChatStream(ctx)
			if err != nil {
				panic(err)
			}
			server.allStreams[uint64(i)] = chatStream

			go func() {
				for {
					msg, err := chatStream.Recv()
					if err != nil {
						fmt.Println("Disconnected...")
						return
					}

					fmt.Println("c: Received message from id '" + strconv.FormatUint(msg.Id, 10) + "' at logical time " + strconv.FormatUint(msg.Timestamp, 10) + " isRequest: " + strconv.FormatBool(msg.IsRequest) + " hasDisconnected: " + strconv.FormatBool(msg.HasDisconnected))

					if msg.IsRequest {
						receive(server, msg)
					} else if msg.HasDisconnected {
						handleDisconnection(server, msg.Id)
					} else {
						grantArrived(server, msg.Id)
					}
				}
			}()

			processes++

		} else if server.id == uint64(i) {
			listener = tmpListener
			//fmt.Println("using: " + address)

		} else {
			tmpListener.Close()
			//fmt.Println(address + " is not used")
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

	server.logger.Info(
		"CURRENT_PROCESSES",
		"amount", processes,
		"node_id", server.id,
	)

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
		disconnectMe(server)
		grpcServer.Stop()
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
		disconnectMe(server)
		grpcServer.Stop()
	}
}

func initialize(s *Server) {
	s.state = "RELEASED"

	s.logger.Info(
		"NODE_IS_INITIALIZED",
		"node_id", s.id,
		"state", s.state,
	)
	//fmt.Println("initializing...")
	go goIdle(s)
}

func goIdle(s *Server) {
	randomDelay(1.0, 2.0)
	s.logger.Info(
		"IDLY_WAITING",
		"node_id", s.id,
		"state", s.state,
		"timestamp", s.lamportTime,
	)

	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()

	go enter(s)
}

func enter(s *Server) {
	s.state = "WANTED"

	s.logger.Info(
		"REQUESTING_CRITICAL_SECTION",
		"node_id", s.id,
		"state", s.state,
		"timestamp", s.lamportTime,
	)

	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()

	msg := &pb.ChatMessage{
		Id:              s.id,
		IsRequest:       true,
		HasDisconnected: false,
		Timestamp:       s.lamportTime,
	}

	//Sends a request to all other processes
	for _, clientStream := range s.allStreams {
		clientStream.Send(msg)

		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	s.logger.Info(
		"SENT_REQUESTS_TO_ALL",
		"node_id", s.id,
		"state", s.state,
		"timestamp", s.lamportTime,
	)

	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()

	//Waits for all processes to grant the request
	waitForGrants(s)

	//Access the Critical Section
	s.state = "HELD"
	go access_cs(s)
}

func waitForGrants(s *Server) {
	//make a map containing all ids of nodes we are waiting for grants from
	// (used so we can skip grants from nodes that later disconnects before sending their grant)
	s.outstandingGrants = make(map[uint64]struct{})
	for id := range s.allStreams {
		s.outstandingGrants[id] = struct{}{}
	}

	s.grantWaitGroup.Add(len(s.allStreams))
	s.grantWaitGroup.Wait()
	s.outstandingGrants = nil
}

func grantArrived(s *Server, id uint64) {
	delete(s.outstandingGrants, id)
	s.grantWaitGroup.Done()
}

func receive(s *Server, msg *pb.ChatMessage) {
	s.logger.Info(
		"REQUEST_RECEIVED",
		"node_id", s.id,
		"peer_id", msg.Id,
		"timestamp", s.lamportTime,
		"peer_timestamp", msg.Timestamp,
	)
	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()

	if s.state == "HELD" ||
		(s.state == "WANTED" && compare_priority(s, msg)) {
		s.queue = append(s.queue, s.allStreams[msg.Id])
		s.logger.Info(
			"REPLY_DEFERRED",
			"node_id", s.id,
			"peer_id", msg.Id,
			"state", s.state,
			"timestamp", s.lamportTime,
		)
	} else {
		s.timeMu.Lock()
		s.lamportTime = max(s.lamportTime, msg.Timestamp) + 1
		s.timeMu.Unlock()
		reply := &pb.ChatMessage{
			Id:              s.id,
			IsRequest:       false,
			HasDisconnected: false,
			Timestamp:       s.lamportTime,
		}

		if err := s.allStreams[msg.Id].Send(reply); err != nil {
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

	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()
}

func exit(s *Server) {

	s.state = "RELEASED"

	s.logger.Info(
		"EXIT_CRITICAL_SECTION",
		"node_id", s.id,
		"state", s.state,
		"timestamp", s.lamportTime,
	)

	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()

	reply := &pb.ChatMessage{
		Id:              s.id,
		IsRequest:       false,
		HasDisconnected: false,
		Timestamp:       s.lamportTime,
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
			"timestamp", s.lamportTime,
		)

		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	//Empty the queue
	s.queue = nil

	go goIdle(s)
}

func access_cs(s *Server) {
	s.logger.Info(
		"ENTER_CRITICAL_SECTION!",
		"node_id", s.id,
		"timestamp", s.lamportTime,
	)

	//The process accesses the Critical Section (here it just prints to the terminal)
	fmt.Println("Process " + strconv.FormatUint(s.id, 10) + " accessed the Critical Section.")
	s.timeMu.Lock()
	s.lamportTime++
	s.timeMu.Unlock()
	randomDelay(0.1, 3.0)
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

func randomDelay(min float64, max float64) {
	span := max - min
	delay := min + rand.Float64()*span
	time.Sleep(time.Duration(delay * float64(time.Second)))
}
func disconnectMe(s *Server) {
	msg := &pb.ChatMessage{
		Id:              s.id,
		IsRequest:       false,
		HasDisconnected: true,
		Timestamp:       s.lamportTime,
	}

	//Sends a disconnect to all other processes
	for _, clientStream := range s.allStreams {
		clientStream.Send(msg)
		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}
}

func handleDisconnection(s *Server, id uint64) {
	fmt.Println("User disconnected. id: " + strconv.FormatUint(id, 10))
	delete(s.allStreams, id)

	s.logger.Info(
		"USER_DISCONNECTED_ID:"+strconv.FormatUint(id, 10),
		"node_id", s.id,
	)

	//in case we are waiting for a grant from the disconnected node, we delete
	// it from the map and decrement the number of grants we are waiting for
	if s.outstandingGrants != nil {
		_, exists := s.outstandingGrants[id]
		if exists {
			delete(s.outstandingGrants, id)
			s.grantWaitGroup.Done()
		}
	}
}

func (s *Server) ChatStream(chatStream pb.ChatService_ChatStreamServer) error {
	//get id from metaData
	md, _ := metadata.FromIncomingContext(chatStream.Context())
	strId := md.Get("id")[0]
	id, _ := strconv.ParseUint(strId, 10, 64)

	s.mu.Lock()
	s.allStreams[id] = chatStream
	s.mu.Unlock()

	s.logger.Info(
		"USER_CONNECTED_ID:"+strconv.FormatUint(id, 10),
		"node_id", s.id,
	)

	for {
		msg, err := chatStream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		fmt.Println("s: Received message from id '" + strconv.FormatUint(msg.Id, 10) + "' at logical time " + strconv.FormatUint(msg.Timestamp, 10) + " isRequest: " + strconv.FormatBool(msg.IsRequest) + " hasDisconnected: " + strconv.FormatBool(msg.HasDisconnected))

		if msg.IsRequest {
			receive(s, msg)
		} else if msg.HasDisconnected {
			handleDisconnection(s, msg.Id)
		} else {
			grantArrived(s, msg.Id)
		}
	}
}
