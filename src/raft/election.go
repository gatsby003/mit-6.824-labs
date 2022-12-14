package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader && rf.election_timeout() {
			rf.handle_timeout()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(RandIntUtil()))
	}
}

func (rf *Raft) election_handler(term int) {
	var votes int32

	majority := int32(len(rf.peers)/2 + 1)
	stop_seeking := make(chan bool, 10)

	atomic.AddInt32(&votes, 1)

	rf.mu.Lock()
	if term != rf.currentTerm || !rf.election_started.Get() {
		rf.Debug(dCanidate, "QUITTING")
		rf.mu.Unlock()
		return
	}

	rf.initiate_seekers(&votes, stop_seeking)

	rf.mu.Unlock()

	rf.watch_majority(stop_seeking, &votes, majority, term)
}

func (rf *Raft) watch_majority(stop_seeking chan bool, votes *int32, majority int32, term int) {
	for {
		select {
		case <-rf.stop_election:
			// stop election
			rf.Debug(dCanidate, "Stopping Election on Server %d for term %d", rf.me, term)
			rf.stop_seekers(stop_seeking)
			return
		default:
			v := atomic.LoadInt32(votes)
			if v >= majority {
				// election won , stop other seeking
				rf.mu.Lock()
				if term == rf.currentTerm && rf.election_started.Get() {
					rf.StopElection()
					rf.isLeader = true
					for i := 0; i < len(rf.peers); i += 1 {
						if i != rf.me {
							go rf.sendHeartBeat(i, rf.currentTerm)
						}
					}
					rf.Debug(dLeader, "Election won sending heartbeats Server %d Term %d", rf.me, rf.currentTerm)
					rf.mu.Unlock()
				} else {
					rf.StopElection()
					rf.mu.Unlock()
					rf.Debug(dCanidate, "Edge Case")
				}
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
			rf.Debug(dCanidate, "Stopping seeking for term %d server %d", term, rf.me)
			return
		default:
			res := RequestVoteReply{}
			if rf.sendRequestVote(server_id, &req, &res) {
				rf.mu.Lock()
				if term != rf.currentTerm || !rf.election_started.Get() {
					rf.mu.Unlock()
					return
				} else if rf.election_started.Get() && rf.currentTerm == res.Term && res.VoteGranted {
					atomic.AddInt32(vote_counter, 1)
					rf.mu.Unlock()
					return
				} else if !res.VoteGranted && res.Term > rf.currentTerm {
					rf.Debug(dCanidate, "QUITTING2")
					rf.StopElection()
					rf.currentTerm = res.Term
					rf.mu.Unlock()
					return
				} else {
					rf.Debug(dVote, "Didnt get vote : %d %d", res.VoteGranted, res.Term)
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

func (rf *Raft) election_timeout() bool {
	return time.Since(rf.electionTimer).Milliseconds() >= rf.timeout.Milliseconds()
}

func (rf *Raft) handle_timeout() {
	if rf.election_started.Get() {
		// election_started = false
		rf.StopElection()
	} else {
		rf.StartElection()
	}
}

func (rf *Raft) StopElection() {
	rf.election_started.Set(false)
	rf.votedFor = -1
	rf.stop_election <- true
	rf.electionTimer = time.Now()
	rf.Debug(dCanidate, "Election Timeout for Server %d Term %d , buffer : %d", rf.me, rf.currentTerm, len(rf.stop_election))
}

func (rf *Raft) StartElection() {
	rf.election_started.Set(true)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimer = time.Now()
	t := rf.currentTerm
	rf.Debug(dFollower, "Starting Election for Server %d Term %d", rf.me, t)
	rf.stop_election = make(chan bool, 1)
	go rf.election_handler(t)
}

func (rf *Raft) stop_seekers(stop_seeking chan bool) {
	for i := 0; i < len(rf.peers); i += 1 {
		stop_seeking <- true
	}
}

func (rf *Raft) initiate_seekers(votes *int32, stop_seeking chan bool) {
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go rf.seekVote(votes, i, rf.currentTerm, rf.me, stop_seeking)
		}
	}
}
