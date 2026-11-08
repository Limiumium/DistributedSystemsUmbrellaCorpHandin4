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
	allStreams        map[uint64]ChatStreamAll
	queue             []ChatStreamAll //Queue of channels
	mu                sync.Mutex
	timeMu            sync.Mutex
	lamportTime       uint64
	state             string
	id                uint64
	outstandingGrants map[uint64]struct{}
	grantWaitGroup    sync.WaitGroup
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

	//create clients foe existing servers and set the listener for our own server
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
		disconnectMe(server)
		grpcServer.Stop()
	}()

	initialize(server)

	grpcServer.Serve(listener)

}

func initialize(s *Server) {
	s.state = "RELEASED"
	//fmt.Println("initialize...")
	go goIdle(s)
}

func goIdle(s *Server) {
	randomDelay(1.0, 2.0)
	go enter(s)
}

func enter(s *Server) {
	s.state = "WANTED"

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
	if s.state == "HELD" ||
		(s.state == "WANTED" && compare_priority(s, msg)) {
		s.queue = append(s.queue, s.allStreams[msg.Id])
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
		s.allStreams[msg.Id].Send(reply)
	}
}

func exit(s *Server) {

	s.state = "RELEASED"
	reply := &pb.ChatMessage{
		Id:              s.id,
		IsRequest:       false,
		HasDisconnected: false,
		Timestamp:       s.lamportTime,
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
