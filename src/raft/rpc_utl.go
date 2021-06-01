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
	log.Printf("follower[%v] is received a rpc[RequestVote] from [%+v], %+v", rf.me, args.CandidateId, rf)

	CandidateTerm := args.Term
	CandidateId := args.CandidateId
	// candidateLastLogIndex :=
	// candidateLastLogTerm :=

	//reply
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.isNotCurrentLead(args.CandidateId) {
		// TODO 进来这里都没有进行处理了，错误逻辑还是在这里。有没有一个checklist？
		if CandidateTerm > rf.currentTerm { // TODO check 下面是 >= (at least as large as), 这里是 >
			reply.VoteGranted = true
			reply.Term = CandidateTerm

			if rf.serverStatus == LEADER {
				log.Printf("[%+v] [incertitude!] current leader term outdated. back To Follower", rf.me)
				// TODO check, 需要更新voteFor吗？update vote??? 需要更换投票吗？如果一个少部分的leader持续不起作用，持续没有majority会发生什么事？看论文...
				rf.backToFollower(CandidateTerm) // candidate 收到 leader 的 heartbeat，停止选举
			} else if rf.serverStatus == CANDIDATE {
				log.Printf("[%+v] [RequestVote] current candidate term outdated. back To Follower", rf.me)
				rf.backToFollower(CandidateTerm) // candidate 收到 leader 的 heartbeat，停止选举
			} else {
				log.Printf("[%+v] [RequestVote] my old leader [%+v] with term [%+v]; current rpc leader [%+v] with term [%+v]. %+v", rf.me, rf.votedFor, rf.currentTerm, CandidateId, CandidateTerm, rf)
			}

			updated := rf.updateCurrentServerStatus(CandidateTerm, CandidateId, "RequestVote")
			if !updated {
				panic("yyy")
			}
			// TODO
		}
		return
	}

	// check
	isVaildTerm := rf.checkInputCandidateTerm(CandidateTerm)
	isVaildCandidate := rf.checkValidCandidateId(CandidateId, CandidateTerm)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm // currentTerm may bigger than candidate's term
	if isVaildTerm && isVaildCandidate {
		reply.VoteGranted = true

		updated := rf.updateCurrentServerStatus(CandidateTerm, CandidateId, "RequestVote")
		if !updated {
			panic("yyy")
		}
	}

	log.Printf("RequestVote Rpc result: \n%+v\n%+v", *args, *reply)
}

//
// example AppendEntries RPC handler.
// 可能是心跳或写入
// 心跳过来改变什么东西？然后会打断什么，然后重新开始睡眠（那个使用什么机制？）
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("follower[%v] receive a rpc[AppendEntries] from [%+v], %+v", rf.me, args.LeaderId, rf)

	// Your code here (2A, 2B).
	leaderTerm := args.Term
	leaderId := args.LeaderId

	// reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// 如果这不是我的 leader's heartbeat，我就
	// 1. check term
	// 	1.1 term 更大，认贼作父？更新term，后面my old leader 过来，我拒绝并把我的term发回去它就都明白了
	//  1.2 term 更小，直接拒绝，并把term返回回去，它就什么都明白了
	if rf.isNotCurrentLead(args.LeaderId) {
		if leaderTerm >= rf.currentTerm {
			if rf.serverStatus == LEADER {
				if leaderTerm == rf.currentTerm {
					panic("brain split!")
				} else {
					log.Printf("[incertitude!] current leader term outdated. back To Follower")
					rf.backToFollower(leaderTerm) // candidate 收到 leader 的 heartbeat，停止选举
				}
			} else if rf.serverStatus == CANDIDATE {
				log.Printf("[AppendEntries] current candidate term outdated. back To Follower")
				rf.backToFollower(leaderTerm) // candidate 收到 leader 的 heartbeat，停止选举
			} else {
				log.Printf("[AppendEntries] my old leader [%+v] with term [%+v]; current rpc leader [%+v] with term [%+v]. %+v", rf.votedFor, rf.currentTerm, leaderId, leaderTerm, rf)
			}
			reply.Success = true
			reply.Term = rf.currentTerm

			updated := rf.updateCurrentServerStatus(leaderTerm, leaderId, "AppendEntries")
			if !updated {
				panic("yyy")
			}
		}
		return
	}

	// check
	isVaildTerm := rf.checkInputCandidateTerm(leaderTerm)

	// reply
	if isVaildTerm {
		reply.Success = true

		updated := rf.updateCurrentServerStatus(leaderTerm, leaderId, "AppendEntries")
		if !updated {
			panic("yyy")
		}
	}

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
