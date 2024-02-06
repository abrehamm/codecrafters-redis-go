package main

import (
	"bytes"
	"encoding/binary"
	"os"
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

func readSingleKeyRDB(file *os.File) string {
	opCode := make([]byte, 1)

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

	file.Read(opCode) //read length of key
	key := make([]byte, int(opCode[0]))
	file.Read(key)

	return string(key)
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
	for {
		file.Read(opCode)
		if int(opCode[0]) == 0xff {
			break
		}
		file.Seek(-1, 1)
		key = readSingleKeyRDB(file)
		value = readSingleValueRDB(file)
		kvStore[key] = value
	}
}
