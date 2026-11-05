package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "chitchat/grpc"
)

var lamportTime uint64

func main() {
	lamportTime = 0

	conn, _ := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(
		insecure.NewCredentials()))

	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	scanner := bufio.NewScanner(os.Stdin)
	//get userName
	userName := ""
	fmt.Print("UserName: ")
	for scanner.Scan() {
		userName = scanner.Text()
		if userName != "" {
			break
		}
	}
	lamportTime++

	//create metaData
	md := metadata.New(map[string]string{
		"username": userName,
	})
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	//connect to chatStream
	chatStream, err := client.ChatStream(ctx)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, err := chatStream.Recv()
			if err != nil {
				fmt.Println("Disconnected")
				return
			}

			lamportTime = max(lamportTime, msg.Timestamp) + 1
			fmt.Println(msg.Username + ": " + msg.Message + " at logical time " + strconv.FormatUint(msg.Timestamp, 10))
		}
	}()

	for scanner.Scan() {
		var input = scanner.Text()
		runes := []rune(input) //"rune" is a predeclared type in go
		if len(runes) > 128 {
			input = string(runes[:128]) //truncate input to 128 characters.
			/*
				So:
				[]rune(input) converts the string to a rune slice.
				[:128] takes at most the first 128 runes.
				string(...) converts the rune slice back to a string.
			*/
		}

		lamportTime++
		msg := &pb.ChatMessage{
			Username:  userName,
			Message:   input,
			Timestamp: lamportTime,
		}

		err := chatStream.Send(msg)

		if err != nil {
			fmt.Println(err)
			break
		}
	}
}
