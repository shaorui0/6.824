package raft

import (
	"log"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
// vote to the comming candidate
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("follower[%v] is received a rpc[RequestVote] from [%v], %v", rf.me, args.CandidateId, rf)

	CandidateTerm := args.Term
	CandidateId := args.CandidateId
	// candidateLastLogIndex :=
	// candidateLastLogTerm :=

	//reply
	if CandidateTerm < rf.currentTerm {
		// reject
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if CandidateTerm > rf.currentTerm {
		rf.backToFollower(CandidateTerm)
	}

	// reply init
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// vote success
	if rf.votedFor == -1 || rf.votedFor == CandidateId {
		reply.VoteGranted = true

		rf.votedFor = CandidateId
		rf.chanGrantVote <- true
	}

	log.Printf("RequestVote Rpc result: \n%+v\n%+v", args, reply)
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("follower[%v] receive a rpc[AppendEntries] from [%v], %v", rf.me, args.LeaderId, rf)

	// Your code here (2A, 2B).
	leaderTerm := args.Term

	if leaderTerm < rf.currentTerm {
		// reject
		reply.Term = rf.currentTerm

		return
	}

	if leaderTerm > rf.currentTerm {
		rf.backToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true
	log.Printf("AppendEntries Rpc result: \n%+v\n%+v", args, reply)
}
