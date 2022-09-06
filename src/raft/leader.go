package raft

import "time"

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		isleader := rf.isLeader
		t := rf.currentTerm
		if isleader {
			// send appendRPC
			for i := 0; i < len(rf.peers); i += 1 {
				if i != rf.me {
					go rf.sendHeartBeat(i, t)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendHeartBeat(peer, term int) {
	rf.mu.Lock()
	if rf.currentTerm != term || !rf.isLeader {
		rf.currentTerm = term
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	req := AppendRPCArgs{
		Term: term,
	}
	reply := AppendRPCReply{}
	rf.sendAppendRPC(peer, &req, &reply)

	rf.mu.Lock()
	if reply.Success {
		rf.Debug(dLeader, "Append RPC successful on server %d by %d for term : %d", peer, rf.me, rf.currentTerm)
	}

	if reply.Term > term {
		// convert to follower
		if rf.isLeader && rf.currentTerm == term {
			rf.isLeader = false
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			// rf.electionTimer = time.Now()
		}
	}
	rf.mu.Unlock()

}
