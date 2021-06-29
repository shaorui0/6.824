package raft

import (
	"log"
)

func (rf *Raft) backToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1 // init
	rf.serverStatus = FOLLOWER
	log.Printf("[%v] backToFollower. %+v\n", rf.me, rf)
}

// func (rf *Raft) upToCandidate() {
// 	rf.serverStatus = CANDIDATE
// 	rf.currentTerm += 1
// 	rf.voteMyself() // vote myself
// 	log.Printf("[%v] upToCandidate. %+v\n", rf.me, rf)
// }

func (rf *Raft) upToLeader() {
	rf.serverStatus = LEADER
	log.Printf("[%+v] upToLeader. %+v\n", rf.me, rf)
}

//
// 1. 没死
// 2. 状态是leader
// 3. voteCount 合适
//
func (rf *Raft) isLeader() bool {
	return rf.dead == 0 &&
		rf.serverStatus == LEADER &&
		rf.votedFor == rf.me &&
		rf.voteCount > len(rf.peers)/2
}
