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

	if reply.Success {
		rf.Debug(dLeader, "Append RPC successful on server %d by %d", peer, rf.me)
	}

	if reply.Term > term {
		// convert to follower
		rf.mu.Lock()
		if rf.isLeader && rf.currentTerm == term {
			rf.isLeader = false
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.electionTimer = time.Now()
		}
		rf.mu.Unlock()

	}
}
