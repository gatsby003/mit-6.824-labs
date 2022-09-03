package raft

import (
	"log"
	"math/rand"
	"sync/atomic"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type TAtomBool struct{ flag int32 }

func (b *TAtomBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), int32(i))
}

func (b *TAtomBool) Get() bool {
	if atomic.LoadInt32(&(b.flag)) != 0 {
		return true
	}
	return false
}

func RandIntUtil() int {
	min := 300
	max := 500

	val := rand.Intn(max-min) + min

	return val

}
