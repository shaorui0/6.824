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

	log.Printf("follower[%v] is received a rpc[RequestVote] from [%+v], %+v", rf.me, args.CandidateId, rf)

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
		rf.backToFollower(args.Term)
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

	log.Printf("RequestVote Rpc result: \n%+v\n%+v", *args, *reply)
}

//
// example AppendEntries RPC handler.
// 可能是心跳或写入
// 心跳过来改变什么东西？然后会打断什么，然后重新开始睡眠（那个使用什么机制？）
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("follower[%v] receive a rpc[AppendEntries] from [%+v], %+v", rf.me, args.LeaderId, rf)

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
	log.Printf("AppendEntries Rpc result: \n%+v\n%+v", *args, *reply)
}

// func (rf *Raft) sleepMicroSecond(ms int) {
// 	// log.Printf("[%+v] start sleep %+v micro seconds", rf.me, ms)
// 	time.Sleep(time.Duration(ms) * time.Millisecond)
// 	// log.Printf("[%+v] end sleep", rf.me)
// }
