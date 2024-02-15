package main

import (
	"flag"
	"strconv"
)

type configType struct {
	dir          string
	dbfilename   string
	port         string
	replicaof    string
	masterReplId string
	offset       int
	masterPort   string
}

func setConfig() *configType {
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

	return &configType{
		dir:          *dir,
		dbfilename:   *dbfilename,
		port:         strconv.Itoa(*port),
		replicaof:    *replicaof,
		masterReplId: masterReplId,
		offset:       offset,
		masterPort:   masterPort,
	}
}
