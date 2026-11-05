package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	chatStreams []pb.ChatService_ChatStreamServer
	mu          sync.Mutex
	timeMu      sync.Mutex
}

var lamportTime uint64
var logger *slog.Logger

func main() {
	lamportTime = 0

	//logger and it's file ("0644" is file permissions)
	file, err := os.OpenFile(
		"server.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	logger = slog.New(
		slog.NewTextHandler(file, nil),
	)

	fmt.Println("Server started")
	logger.Info("Server started", "component", "Server", "LTime", lamportTime)

	server := &Server{}

	listener, _ := net.Listen("tcp", "localhost:5050")
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

		server.timeMu.Lock()
		lamportTime++
		server.timeMu.Unlock()
		fmt.Println("Server stopped")
		logger.Info("Server stopped", "component", "Server", "LTime", lamportTime)

		grpcServer.Stop()
	}()

	grpcServer.Serve(listener)
}

func (s *Server) ChatStream(chatStream pb.ChatService_ChatStreamServer) error {
	//get userName from metaData
	md, _ := metadata.FromIncomingContext(chatStream.Context())
	userName := md.Get("username")[0]

	s.mu.Lock()
	s.chatStreams = append(s.chatStreams, chatStream)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, st := range s.chatStreams {
			if st == chatStream {
				s.chatStreams = append(s.chatStreams[:i], s.chatStreams[i+1:]...)
				break
			}
		}
		lamportTime++
		logger.Info("Client disconnected", "component", "Client", "ClientID", userName, "LTime", lamportTime)

		fmt.Println("Client '" + userName + "' disconnected")
		//fmt.Println("nb users:" + strconv.Itoa(len(s.chatStreams)))
		lamportTime++
		msg := &pb.ChatMessage{
			Username:  "Server",
			Message:   "Participant '" + userName + "' left Chit Chat",
			Timestamp: lamportTime,
		}
		logger.Info("Client disconnected message sent", "component", "Server", "ClientID", userName, "LTime", lamportTime)

		for _, clientStream := range s.chatStreams {
			clientStream.Send(msg)
		}
	}()

	s.timeMu.Lock()
	lamportTime++
	logger.Info("Client connected", "component", "Client", "ClientID", userName, "LTime", lamportTime)
	fmt.Println("Client '" + userName + "' connected")
	//fmt.Println("nb users:" + strconv.Itoa(len(s.chatStreams)))
	lamportTime++
	msg := &pb.ChatMessage{
		Username:  "Server",
		Message:   "Participant '" + userName + "' joined Chit Chat",
		Timestamp: lamportTime,
	}
	logger.Info("Client connected message sent", "component", "Server", "ClientID", userName, "LTime", lamportTime)
	s.timeMu.Unlock()
	sendMessageToAll(s, msg)

	for {
		msg, err := chatStream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		s.timeMu.Lock()
		lamportTime = max(lamportTime, msg.Timestamp) + 1
		logger.Info("Client message received", "component", "Client", "ClientID", userName, "message", msg.Message, "LTime", lamportTime)

		lamportTime++
		msg.Timestamp = lamportTime
		fmt.Println(msg.Username + ": " + msg.Message + " at logical time " + strconv.FormatUint(lamportTime, 10))
		logger.Info("Client message sent", "component", "Server", "ClientID", userName, "LTime", lamportTime)
		s.timeMu.Unlock()

		sendMessageToAll(s, msg)
	}
}

func sendMessageToAll(s *Server, msg *pb.ChatMessage) {
	s.mu.Lock()
	for _, clientStream := range s.chatStreams {
		clientStream.Send(msg)
	}
	s.mu.Unlock()
}
