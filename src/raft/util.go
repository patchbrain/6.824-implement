package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(a, b int) (min int) {
	min = a
	if b < a {
		min = b
	}
	return
}
