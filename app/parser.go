package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

func skipUntilKV(file *os.File) {
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
}

func readSingleKeyRDB(file *os.File) (string, bool) {
	isExpired := true
	key := ""

	// expiry time params
	opCode := make([]byte, 1)
	file.Read(opCode)
	timerOpCode := int(opCode[0])
	timeByteLengths := map[int]int{0x00: 0, 0xfd: 4, 0xfc: 8}
	timeBytes := make([]byte, timeByteLengths[timerOpCode])
	file.Read(timeBytes)
	var unixStamp int64
	buf := bytes.NewReader(timeBytes)
	binary.Read(buf, binary.LittleEndian, &unixStamp)
	if int(opCode[0]) == 0xfc {
		unixStamp /= 1000
	}
	expiryTime := time.Unix(unixStamp, 0)
	if timerOpCode != 0 {
		file.Read(opCode)
	} // if either FC or FD are encountered, the next byte after timestamp bytes is value type code
	file.Read(opCode) //read length of key
	keyBytes := make([]byte, int(opCode[0]))
	file.Read(keyBytes)
	key = string(keyBytes)
	fmt.Println(key)
	if timerOpCode == 0 || expiryTime.After(time.Now()) {
		isExpired = false
	} // timerOpCode == 0 is for keys with no expiry set
	return key, isExpired

}

func readSingleValueRDB(file *os.File) string {
	lenEncoding := make([]byte, 1)
	file.Read(lenEncoding)
	lenMask := 0xc0 // the two msbits determining length encoding

	var lenValue int32
	switch lenMask & int(lenEncoding[0]) {
	case 0x00:
		lenValue = int32(lenEncoding[0])
	case 0x40:
		lenBytes := make([]byte, 2)
		file.Read(lenBytes)
		var allBytes int32
		buf := bytes.NewReader(lenBytes)
		binary.Read(buf, binary.BigEndian, &allBytes)
		lenValue = allBytes & 0x00003fff // to get the 6 lsbits of first byte & combine with next byte
	case 0x80:
		file.Seek(1, 1)
		lenBytes := make([]byte, 4)
		file.Read(lenBytes)
		var allBytes int32
		buf := bytes.NewReader(lenBytes)
		binary.Read(buf, binary.BigEndian, &allBytes)
		lenValue = allBytes
	case 0xc0:
		unmasked := int32(lenEncoding[0] & 0x3f)
		switch unmasked {
		case 0:
			lenValue = 1
		case 1:
			lenValue = 2
		case 2:
			lenValue = 4
		}
	}
	value := make([]byte, lenValue)
	file.Read(value)
	return string(value)
}

func parseRDBToStore(file *os.File, kvStore map[string]string) {
	opCode := make([]byte, 1)
	skipUntilKV(file)
	var key, value string
	var isExpired bool
	for {
		isExpired = false
		file.Read(opCode)
		if int(opCode[0]) == 0xff {
			break
		}
		file.Seek(-1, 1)
		key, isExpired = readSingleKeyRDB(file)
		value = readSingleValueRDB(file)
		if !isExpired {
			kvStore[key] = value
		}
	}
}
