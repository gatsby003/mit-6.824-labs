package raft

import "time"

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		isleader := rf.isLeader
		if isleader {
			// send appendRPC
			for i := 0; i < len(rf.peers); i += 1 {
				if i != rf.me {
					go rf.sendHeartBeat(i, rf.currentTerm)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendHeartBeat(peer, term int) {
	req := AppendRPCArgs{
		Term: term,
	}
	reply := AppendRPCReply{}
	rf.sendAppendRPC(peer, &req, &reply)
}
