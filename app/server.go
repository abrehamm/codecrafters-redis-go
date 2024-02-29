package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"net"
	"os"
)

func main() {

	config := setConfig()
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

	listener, err := net.Listen("tcp", "0.0.0.0:"+config.port)
	if err != nil {
		fmt.Println("Failed to bind to port " + config.port)
		os.Exit(1)
	}
	fmt.Println("Listening on port: " + config.port)

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
		// masterConn.Read(buff)
		// time.Sleep(10 * time.Millisecond)
		defer masterConn.Close()
		go handleRequest(masterConn, *config, kvStore)

	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn, *config, kvStore)
	}

}
func handleRequest(conn net.Conn, config configType, kvStore map[string]string) {

	for {
		buff := make([]byte, 1024)
		nBytes, err := conn.Read(buff)
		// fmt.Println(strings.Split(string(buff[:nBytes]), ("\r\n")))

		// if nBytes == 0 || string(buff[0]) != "*" { // wait only for commands
		// 	continue
		// }
		if nBytes == 0 || err != nil {
			// if err.Error() == "EOF" {
			// 	continue
			// }
			conn.Close()
			break
		}

		chunks := strings.Split(string(buff[:nBytes]), ("\r\n"))
		if len(chunks) < 4 {
			continue
		}
		fmt.Println("Recieved[str]: ", chunks)
		for j := 0; j < len(chunks)-1; {
			command := make([]string, 0)
			nagrs, er := strconv.Atoi(chunks[j][1:])
			if er != nil {
				j += 1
				continue
			}
			for i := 0; i < nagrs; i++ {
				command = append(command, chunks[j+(i*2)+2])
			}
			j += (nagrs * 2) + 1
			fmt.Println(command)
			handleCommand(command, conn, config, kvStore)
		}
	}
}

func handleCommand(command []string, conn net.Conn, config configType, kvStore map[string]string) {
	switch strings.ToUpper(command[0]) {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		resp := "+" + command[1] + "\r\n"
		conn.Write([]byte(resp))
	case "SET":
		kvStore[command[1]] = command[2]
		if len(command) > 3 {
			switch strings.ToUpper(command[3]) {
			case "PX":
				ttl, _ := strconv.Atoi(command[4])
				go func() {
					<-time.After(time.Duration(ttl) * time.Millisecond)
					delete(kvStore, command[1])
				}()
			}
		}
		if config.replicaof == "" {
			conn.Write([]byte("+OK\r\n"))
			for _, s := range config.slaves {
				commandResp := formatRESP(command, "array")
				s.Write([]byte(commandResp))
			}
		}

	case "GET":
		val := kvStore[command[1]]
		if val == "" {
			conn.Write([]byte(NULLSTRING))
		} else {
			resp := formatRESP([]string{val}, "simpleString")
			conn.Write([]byte(resp))
		}
	case "CONFIG":
		if strings.ToUpper(command[1]) == "GET" && strings.ToUpper(command[2]) == "DIR" {
			if config.dir == "" {
				conn.Write([]byte(NULLSTRING))
			} else {
				resp := formatRESP([]string{"dir", config.dir}, "array")
				conn.Write([]byte(resp))
			}
		}
	case "KEYS":
		ks := make([]string, 0)
		for k := range kvStore {
			ks = append(ks, k)
		}
		resp := formatRESP(ks, "array")
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
	case "REPLCONF":
		if len(command) > 1 && strings.ToUpper(command[1]) == "GETACK" {
			data := []string{
				"REPLCONF",
				"ACK",
				"0",
			}
			resp := formatRESP(data, "array")
			conn.Write([]byte(resp))
		} else {
			config.slaves[conn.RemoteAddr().String()] = conn
			conn.Write([]byte(formatRESP([]string{"OK"}, "simpleString")))
		}
	case "PSYNC":
		commandResp := formatRESP(
			[]string{
				"FULLRESYNC",
				config.masterReplId,
				strconv.Itoa(config.offset)},
			"simpleString")
		conn.Write([]byte(commandResp))
		emptyB64RDB := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
		file, _ := base64.StdEncoding.DecodeString(emptyB64RDB)
		resp := append([]byte("$"+strconv.Itoa(len(file))+"\r\n"), file...)
		conn.Write(resp)
	}
}
