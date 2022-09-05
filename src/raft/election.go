package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) ticker() {
	// election_started := false
	// stop_election := make(chan bool, 10)
	random_time := time.Millisecond * time.Duration(RandIntUtil())
	rf.Debug(dCanidate, random_time.String())
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		if !rf.isLeader && time.Since(rf.electionTimer).Milliseconds() > random_time.Milliseconds() {
			// timeout period passed
			// start election
			if rf.election_started.Get() {
				// election_started = false
				rf.election_started.Set(false)
				rf.votedFor = -1
				rf.stop_election <- true
				rf.Debug(dCanidate, "Election Timeout for Server %d Term %d", rf.me, rf.currentTerm)
			} else {
				rf.election_started.Set(true)
				rf.currentTerm = rf.currentTerm + 1
				rf.votedFor = rf.me
				rf.electionTimer = time.Now()
				go rf.startElection()
				rf.Debug(dFollower, "Starting Election for Server %d Term %d", rf.me, rf.currentTerm)

			}

		}
		rf.mu.Unlock()

		// if time greater than a fixed period , start election
		// else sleep for a random time
		// and try again
		time.Sleep(time.Millisecond * time.Duration(RandIntUtil()))

	}
}

func (rf *Raft) startElection() {
	var votes int32
	stop_seeking := make(chan bool, 10)
	votes += 1

	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go rf.seekVote(&votes, i, rf.currentTerm, rf.me, stop_seeking)
		}
	}

	majority := int32(len(rf.peers)/2 + 1)

	rf.mu.Unlock()

	for {
		select {
		case <-rf.stop_election:
			// stop election
			for i := 0; i < len(rf.peers); i += 1 {
				stop_seeking <- true
			}
			return
		default:
			v := atomic.LoadInt32(&votes)
			if v >= majority {
				// election won , stop other seeking
				rf.mu.Lock()
				rf.isLeader = true
				for i := 0; i < len(rf.peers); i += 1 {
					if i != rf.me {
						go rf.sendHeartBeat(i, rf.currentTerm)
					}
				}
				rf.Debug(dLeader, "Election won sending heartbeats Server %d Term %d", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) seekVote(vote_counter *int32, server_id int, term int, me int, stop_seeking chan bool) {
	req := RequestVoteArgs{
		Term:        term,
		CandidateId: me,
	}

	for {
		select {
		case <-stop_seeking:
			return
		default:
			res := RequestVoteReply{}
			if rf.sendRequestVote(server_id, &req, &res) {
				if res.VoteGranted {
					atomic.AddInt32(vote_counter, 1)
					return
				}
			}
		}
	}
}
