package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {

	dir := flag.String("dir", "", "Dirctory of RDB")
	dbfilename := flag.String("dbfilename", "", "File Name of RDB")

	flag.Parse()
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
		go handleRequest(conn, *dir, *dbfilename)
	}

}
func handleRequest(conn net.Conn, dir string, dbfilename string) {
	kvStore := make(map[string]string)
	kvStore["dir"] = dir
	kvStore["dbfilename"] = dbfilename

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
			fmt.Println(kvStore)
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
				val := kvStore["dir"]
				if val == "" {
					conn.Write([]byte("$" + strconv.Itoa(-1) + "\r\n"))
				} else {
					resp := "*2" + "\r\n" + "$3\r\ndir\r\n$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
					conn.Write([]byte(resp))
				}
			}
		case "KEYS":
			file, err := os.Open(dir + "/" + dbfilename)
			if err != nil {
				fmt.Println("Error reading RDB file: ", err.Error())
			} else {
				key := readSingleKey(file)
				resp := "*1" + "\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n"
				conn.Write([]byte(resp))
				file.Close()
			}
		}
	}
}

func readSingleKey(file *os.File) string {
	opCode := make([]byte, 1)
	// Assuming single db & redis version > 7, RESIZEDB opcode is the closest to keys
	// skip all bytes until the first 0xfb is reached
	for file.Read(opCode); opCode[0] != 0xfb; file.Read(opCode) {
	}

	// skip RESIZEDB params
	lenMask := 0xc0 // the two msbits determining length encoding
	for i := 0; i < 2; i++ {
		file.Read(opCode)
		switch lenMask & int(opCode[0]) {
		case 0x00:
			file.Seek(0, 1)
		case 0x40:
			file.Seek(1, 1)
		case 0x80:
			file.Seek(4, 1)
		}
	}

	// skip expiry time params
	file.Read(opCode)
	switch int(opCode[0]) {
	case 0x00:
		file.Seek(0, 1)
	case 0xfd:
		file.Seek(4, 1)
	case 0xfc:
		file.Seek(8, 1)
	}

	//read length of key
	file.Read(opCode)

	key := make([]byte, int(opCode[0]))
	file.Read(key)

	return string(key)
}
