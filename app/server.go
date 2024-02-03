package main

import (
	"bytes"
	"fmt"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn)
	}

}
func handleRequest(conn net.Conn) {
	for {
		buff := make([]byte, 1024)
		nBytes, err := conn.Read(buff)
		if err != nil || nBytes == 0 {
			conn.Close()
			break
		}
		// for i := 0; i < nBytes; i++{

		// }
		fmt.Println("Recieved[raw]: ", buff[:nBytes])
		runes := bytes.Split(buff[:nBytes], []byte("\r\n"))
		if string(runes[2]) == "echo" || string(runes[2]) == "ECHO" {
			resp := "+" + string(runes[4]) + "\r\n"
			conn.Write([]byte(resp))
		}
		conn.Write([]byte("+PONG\r\n"))
	}

}
