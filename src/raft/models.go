package raft

type AppendRPCArgs struct {
	Term     int
	LeaderId int
}

type AppendRPCReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}
