package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) RunServer() {
	log.Printf("RunServer...\nInit Status: %+v", rf)
	for {
		if rf.killed() {
			log.Printf("[%v] was been killed", rf.me)
			break
		}

		switch rf.serverStatus {
		case FOLLOWER:
			select {
			case <-rf.chanHeartbeat:
			case <-rf.chanGrantVote:
			case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				rf.serverStatus = CANDIDATE
			}
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1 // vote itself
			rf.mu.Unlock()
			log.Printf("%v become CANDIDATE %v\n", rf.me, rf.currentTerm)
			go rf.askVoteToAllPeer()

			//check
			select {
			case <-rf.chanHeartbeat:
				log.Printf("CANDIDATE %v reveive chanHeartbeat\n", rf.me)
				rf.serverStatus = FOLLOWER
			case <-rf.chanWinElect: // 确认了可以成为leader
				rf.mu.Lock()
				rf.upToLeader()
				// TODO log
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):

			}
		case LEADER:
			go rf.askHeartbeatToAllPeer()
			time.Sleep(time.Millisecond * 60)
		}
	}
}

func (rf *Raft) askVoteToAllPeer() {
	rf.mu.Lock()
	log.Printf("[%v] askVoteToAllPeer start\n", rf.me)
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.serverStatus == CANDIDATE {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
	log.Printf("[%v] askVoteToAllPeer end\n", rf.me)
}

func (rf *Raft) askHeartbeatToAllPeer() {
	rf.mu.Lock()
	log.Printf("[%v] askHeartbeatToAllPeer start\n", rf.me)
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.serverStatus == LEADER {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
	log.Printf("[%v] askHeartbeatToAllPeer end\n", rf.me)
}
