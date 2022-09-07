package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		// become follower : either stop election or step down as leader
		t := rf.currentTerm
		rf.currentTerm = args.Term
		if rf.election_started.Get() {
			rf.StopElection()
			rf.Debug(dCanidate, "RQV : Stopping Election Server %d since term is lower New Term %d Old Term %d", rf.me, rf.currentTerm, t)
		}
		if rf.isLeader {
			rf.isLeader = false
			rf.Debug(dLeader, "RQV : Stepping Down as leader %d new term:  %d old term: %d", rf.me, rf.currentTerm, t)
		}

		// vote for the guy !!
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimer = time.Now()
		reply.Term = rf.currentTerm

		rf.Debug(dFollower, "Server %d voted for %d for term %d", rf.me, args.CandidateId, args.Term)

	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Debug(dVote, "No vote for %d by %d for term %d as ref.currentTerm %d", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !rf.isLeader {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimer = time.Now()
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.Debug(dFollower, "server %d voted for %d for term %d", rf.me, args.CandidateId, args.Term)
	} else {
		rf.Debug(dVote, "Not Voted voted for : %d, term : %d, argTerm : %d asked to %d", args.CandidateId, rf.currentTerm, args.Term, rf.me)
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendRPC(args *AppendRPCArgs, reply *AppendRPCReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		// become follower : either stop election or step down as leader
		t := rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if rf.election_started.Get() {
			rf.StopElection()
			rf.Debug(dCanidate, "APRPC Stopping Election Server %d since term is lower New Term %d Old Term %d", rf.me, rf.currentTerm, t)
		}
		if rf.isLeader {
			rf.votedFor = -1
			rf.isLeader = false
			rf.Debug(dLeader, "APRPC :Stepping Down as leader %d new term:  %d old term: %d", rf.me, rf.currentTerm, t)
		}
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.electionTimer = time.Now()
		if rf.election_started.Get() {
			rf.StopElection()
		}
		fmt.Println(".......")
	}

	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendRPC(server int, args *AppendRPCArgs, reply *AppendRPCReply) bool {
	ok := rf.peers[server].Call("Raft.AppendRPC", args, reply)
	return ok
}
