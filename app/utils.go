package main

import (
	"fmt"
)

// formatRESP returns RESP formatted string of params passed in data.
//
//	data - array of strings to be returned as response
//	format - "buklString" for now
func formatRESP(data []string, format string) string {
	if format == "" {
		format = "bulkString"
	}
	resp := ""
	switch format {
	case "array":
		resp = fmt.Sprintf("*%d\r\n", len(data))
		for _, d := range data {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(d), d)
		}
	case "bulkString":
		for _, d := range data {
			resp += fmt.Sprintf("%s\r\n", d)
		}
		resp = fmt.Sprintf("$%d\r\n%s", len(resp)-2, resp) // -2 to account for the last CRLF
	}
	return resp
}
