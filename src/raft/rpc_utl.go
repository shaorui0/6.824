package raft

import (
	"log"
	"time"
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
		// trans to follower
		rf.serverStatus = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
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
	leaderId := args.LeaderId

	if leaderTerm < rf.currentTerm {
		// reject
		reply.Term = rf.currentTerm

		return
	}

	if leaderTerm > rf.currentTerm {
		// trans to follower
		rf.serverStatus = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true
	reply.Term = rf.currentTerm

	// TODO log
	reply.Success = true

	log.Printf("AppendEntries Rpc result: \n%+v\n%+v", *args, *reply)
}

//
// update server's status when success
//
func (rf *Raft) updateCurrentServerStatus(newTerm int, newVotedFor int, rpcType string) bool {
	if rpcType == "RequestVote" {
		rf.receivedRequestVote = true
	} else if rpcType == "AppendEntries" {
		rf.receivedAppendEntries = true
	} else {
		return false
	}

	if newTerm != rf.currentTerm {
		log.Printf("[%+v] changed currentTerm from [%+v] to [%+v]", rf.me, rf.currentTerm, newTerm)
	}
	rf.currentTerm = newTerm

	if newVotedFor != rf.votedFor {
		log.Printf("[%+v] changed votedFor from [%+v] to [%+v]", rf.me, rf.votedFor, newVotedFor)
	}
	rf.votedFor = newVotedFor
	return true
}

//
// check this server's term
// 过来的term必须比我的大，我才能接收
//
func (rf *Raft) checkInputCandidateTerm(term int) bool {
	if term < rf.currentTerm {
		return false
	}

	if (rf.serverStatus == LEADER || rf.serverStatus == CANDIDATE) && term > rf.currentTerm {
		log.Printf("leader/candidate[%v]'s term[%v] is outdated, back to follower. %+v", rf.me, rf.currentTerm, rf)
		rf.backToFollower(term)
	}

	return true
}

//
// votedFor
// lastLogTerm > , true
// lastLogTerm == ,  check lastLogIndex, true
//
func (rf *Raft) checkValidCandidateId(candidateId int, term int) bool {
	// 1. check
	if rf.isNotCurrentLead(candidateId) {
		return false
	}
	// TODO 1.2 rf.lastLogTerm

	return true
}

func (rf *Raft) sleepMicroSecond(ms int) {
	// log.Printf("[%+v] start sleep %+v micro seconds", rf.me, ms)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	// log.Printf("[%+v] end sleep", rf.me)
}
