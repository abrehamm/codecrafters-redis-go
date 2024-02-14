package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"net"
	"os"
)

func main() {

	dir := flag.String("dir", "", "Dirctory of RDB")
	dbfilename := flag.String("dbfilename", "", "File Name of RDB")
	port := flag.Int("port", 6379, "Port number")
	replicaof := flag.String("replicaof", "", "Master server")
	masterReplId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	offset := 0
	masterPort := ""

	flag.Parse()
	if args := flag.Args(); len(args) > 0 {
		masterPort = args[0]
	}
	portString := strconv.Itoa(*port)
	listener, err := net.Listen("tcp", "0.0.0.0:"+portString)
	if err != nil {
		fmt.Println("Failed to bind to port " + portString)
		os.Exit(1)
	}

	if *replicaof != "" {
		masterConn, err := net.Dial("tcp", *replicaof+":"+masterPort)
		if err != nil {
			fmt.Println("Failed to PING master at " + *replicaof + ":" + masterPort)
			os.Exit(1)
		}
		masterConn.Write([]byte("*1\r\n$4\r\nping\r\n"))
		defer masterConn.Close()
	}

	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn, *dir, *dbfilename, *replicaof, masterReplId, offset)
	}

}
func handleRequest(conn net.Conn, dir string, dbfilename string, replicaof string, replId string, offset int) {
	kvStore := make(map[string]string)

	if dbfilename != "" {
		file, err := os.Open(dir + "/" + dbfilename)
		if err != nil {
			fmt.Println("Error reading RDB file: ", err.Error())
		} else {
			parseRDBToStore(file, kvStore)
		}
		defer file.Close()
	}
	for {
		buff := make([]byte, 1024)
		nBytes, err := conn.Read(buff)

		if err != nil || nBytes == 0 {
			conn.Close()
			break
		}

		fmt.Println("Recieved[raw]: ", buff[:nBytes])
		chunks := strings.Split(string(buff[:nBytes]), ("\r\n"))
		fmt.Println("Recieved[str]: ", chunks)
		command := strings.ToUpper(chunks[2])
		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			resp := "+" + chunks[4] + "\r\n"
			conn.Write([]byte(resp))
		case "SET":
			kvStore[chunks[4]] = chunks[6]
			if len(chunks) > 8 {
				param := strings.ToUpper(chunks[8])
				switch param {
				case "PX":
					ttl, _ := strconv.Atoi(chunks[10])
					go func() {
						<-time.After(time.Duration(ttl) * time.Millisecond)
						delete(kvStore, chunks[4])
					}()
				}
			}
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			val := kvStore[chunks[4]]
			if val == "" {
				conn.Write([]byte("$" + strconv.Itoa(-1) + "\r\n"))
			} else {
				conn.Write([]byte("+" + val + "\r\n"))
			}
		case "CONFIG":
			if strings.ToUpper(chunks[4]) == "GET" && strings.ToUpper(chunks[6]) == "DIR" {
				if dir == "" {
					conn.Write([]byte("$" + strconv.Itoa(-1) + "\r\n"))
				} else {
					resp := "*2" + "\r\n" + "$3\r\ndir\r\n$" + strconv.Itoa(len(dir)) + "\r\n" + dir + "\r\n"
					conn.Write([]byte(resp))
				}
			}
		case "KEYS":
			ks := make([]string, 0)
			for k := range kvStore {
				ks = append(ks, "$"+strconv.Itoa(len(k)), k)
			}
			resp := "*" + strconv.Itoa(len(kvStore)) + "\r\n"
			resp += strings.Join(ks, "\r\n") + "\r\n"
			conn.Write([]byte(resp))
		case "INFO":
			role := "master"
			if replicaof != "" {
				role = "slave"
			}
			data := []string{
				"role:" + role,
				"master_replid:" + replId,
				"master_repl_offset:" + strconv.Itoa(offset),
			}
			resp := formatRESP(data, "bulkString")
			conn.Write([]byte(resp))

		}
	}
}
