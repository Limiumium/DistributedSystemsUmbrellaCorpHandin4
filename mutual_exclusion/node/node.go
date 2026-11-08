package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
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
	allStreams     map[uint64]ChatStreamAll
	queue          []ChatStreamAll //Queue of channels
	mu             sync.Mutex
	timeMu         sync.Mutex
	lamportTime    uint64
	state          string
	id             uint64
	grantWaitGroup sync.WaitGroup
}

func main() {
	var MAX_NB_NODES = 10
	server := &Server{}
	server.allStreams = make(map[uint64]ChatStreamAll)

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

	var listener net.Listener
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
						fmt.Println("Disconnected")
						return
					}

					fmt.Println("c: Received message from id '" + strconv.FormatUint(msg.Id, 10) + "' at logical time " + strconv.FormatUint(msg.Timestamp, 10) + " isRequest: " + strconv.FormatBool(msg.IsRequest))

					if msg.IsRequest {
						receive(server, msg)
					} else {
						grantArrived(server)
					}
				}
			}()

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

	initialize(server)

	grpcServer.Serve(listener)

}

func initialize(s *Server) {
	s.state = "RELEASED"
	fmt.Println("initialize...")
	go goIdle(s)
}

func goIdle(s *Server) {
	randomDelay(1.0, 2.0)
	go enter(s)
}

func enter(s *Server) {
	s.state = "WANTED"

	msg := &pb.ChatMessage{
		Id:        s.id,
		IsRequest: true,
		Timestamp: s.lamportTime,
	}

	//Sends a request to all other processes
	for _, clientStream := range s.allStreams {
		clientStream.Send(msg)
		s.timeMu.Lock()
		s.lamportTime++
		s.timeMu.Unlock()
	}

	//Waits for all processes to grant the request
	waitForGrants(s)

	//Access the Critical Section
	s.state = "HELD"
	go access_cs(s)
}

func waitForGrants(s *Server) {
	s.grantWaitGroup.Add(len(s.allStreams))
	s.grantWaitGroup.Wait()
}

func grantArrived(s *Server) {
	s.grantWaitGroup.Done()
}

func receive(s *Server, msg *pb.ChatMessage) {
	if s.state == "HELD" ||
		(s.state == "WANTED" && compare_priority(s, msg)) {
		s.queue = append(s.queue, s.allStreams[msg.Id])
	} else {
		s.timeMu.Lock()
		s.lamportTime = max(s.lamportTime, msg.Timestamp) + 1
		s.timeMu.Unlock()
		reply := &pb.ChatMessage{
			Id:        s.id,
			IsRequest: false,
			Timestamp: s.lamportTime,
		}
		s.allStreams[msg.Id].Send(reply)
	}
}

func exit(s *Server) {

	s.state = "RELEASED"
	reply := &pb.ChatMessage{
		Id:        s.id,
		IsRequest: false,
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

	go goIdle(s)
}

func access_cs(s *Server) {
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

func (s *Server) ChatStream(chatStream pb.ChatService_ChatStreamServer) error {
	//get id from metaData
	md, _ := metadata.FromIncomingContext(chatStream.Context())
	strId := md.Get("id")[0]
	id, _ := strconv.ParseUint(strId, 10, 64)

	s.mu.Lock()
	s.allStreams[id] = chatStream
	s.mu.Unlock()

	/*defer func() {
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
	}()*/

	/*s.timeMu.Lock()
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
	sendMessageToAll(s, msg)*/

	for {
		msg, err := chatStream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		fmt.Println("s: Received message from id '" + strconv.FormatUint(msg.Id, 10) + "' at logical time " + strconv.FormatUint(msg.Timestamp, 10) + " isRequest: " + strconv.FormatBool(msg.IsRequest))

		if msg.IsRequest {
			receive(s, msg)
		} else {
			grantArrived(s)
		}

		/*s.timeMu.Lock()
		lamportTime = max(lamportTime, msg.Timestamp) + 1
		logger.Info("Client message received", "component", "Client", "ClientID", userName, "message", msg.Message, "LTime", lamportTime)

		lamportTime++
		msg.Timestamp = lamportTime
		fmt.Println(msg.Username + ": " + msg.Message + " at logical time " + strconv.FormatUint(lamportTime, 10))
		logger.Info("Client message sent", "component", "Server", "ClientID", userName, "LTime", lamportTime)
		s.timeMu.Unlock()

		sendMessageToAll(s, msg)*/
	}
}
