package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"net"
	"os"
)

func main() {

	config := setConfig()

	listener, err := net.Listen("tcp", "0.0.0.0:"+config.port)
	if err != nil {
		fmt.Println("Failed to bind to port " + config.port)
		os.Exit(1)
	}

	if config.replicaof != "" {
		buff := make([]byte, 1024)
		masterConn, err := net.Dial("tcp", config.replicaof+":"+config.masterPort)
		if err != nil {
			fmt.Println("Failed to PING master at " + config.replicaof + ":" + config.masterPort)
			os.Exit(1)
		}
		// handshake with master
		command := formatRESP([]string{"ping"}, "array")
		masterConn.Write([]byte(command))
		masterConn.Read(buff)
		command = formatRESP([]string{"REPLCONF", "listening-port", config.port}, "array")
		masterConn.Write([]byte(command))
		masterConn.Read(buff)
		command = formatRESP([]string{"REPLCONF", "capa", "psync2"}, "array")
		masterConn.Write([]byte(command))
		masterConn.Read(buff)
		command = formatRESP([]string{"PSYNC", "?", "-1"}, "array")
		masterConn.Write([]byte(command))
		masterConn.Read(buff)
		defer masterConn.Close()
	}

	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn, *config)
	}

}
func handleRequest(conn net.Conn, config configType) {
	kvStore := make(map[string]string)

	if config.dbfilename != "" {
		file, err := os.Open(config.dir + "/" + config.dbfilename)
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
				if config.dir == "" {
					conn.Write([]byte("$" + strconv.Itoa(-1) + "\r\n"))
				} else {
					resp := "*2" + "\r\n" + "$3\r\ndir\r\n$" + strconv.Itoa(len(config.dir)) + "\r\n" + config.dir + "\r\n"
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
			if config.replicaof != "" {
				role = "slave"
			}
			data := []string{
				"role:" + role,
				"master_replid:" + config.masterReplId,
				"master_repl_offset:" + strconv.Itoa(config.offset),
			}
			resp := formatRESP(data, "bulkString")
			conn.Write([]byte(resp))

		}
	}
}
