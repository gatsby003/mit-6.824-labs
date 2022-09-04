package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type logTopic string

const (
	dVote       logTopic = "VOTE"
	dHeartBeat  logTopic = "HEARTBEAT"
	dResetTimer logTopic = "TIMER"
	dLeader     logTopic = "LEADER"
	dFollower   logTopic = "FOLLOWER"
	dCanidate   logTopic = "CANDIDATE"
)

// Debugging

func (rf *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	if rf.debug {
		time := time.Since(rf.start_since).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
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
