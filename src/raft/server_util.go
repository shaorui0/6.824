package raft

import (
	"log"
	"math/rand"
	"time"
)

func (rf *Raft) backToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1 // init
	rf.serverStatus = FOLLOWER
	rf.voteTimeout = rf.generateNewTimeout("RequestVote")
	rf.followerCount = 0
	log.Printf("[%+v] backToFollower. %+v\n", rf.me, rf)
}

func (rf *Raft) upToCandidate() {
	rf.serverStatus = CANDIDATE
	rf.currentTerm += 1
	rf.voteMyself() // vote myself
	log.Printf("[%+v] upToCandidate. %+v\n", rf.me, rf)
}

func (rf *Raft) upToLeader() {
	rf.serverStatus = LEADER
	log.Printf("[%+v] upToLeader. %+v\n", rf.me, rf)
}

func (rf *Raft) generateNewTimeout(rpcType string) int {
	if rpcType == "RequestVote" {
		rand.Seed(time.Now().Unix() + int64(rf.me*100))
		rangeLower := 200
		rangeUpper := 400
		randomNum := rangeLower + rand.Intn(rangeUpper-rangeLower+1)
		log.Printf("[%v] generateNewTimeout: %v\n", rf.me, randomNum)
		return randomNum // FIXME random 300 - 500 ?
	} else if rpcType == "AppendEntries" {
		return HEARTBEAT // 无需更新另一个voteTimeout，返回一个常数即可
	} else {
		panic("generate NewTimeout")
	}
	return -1
}

//
// 1. 没死
// 2. 状态是leader
// 3. followerCount 合适
//
func (rf *Raft) isLeader() bool {
	return rf.dead == 0 &&
		rf.serverStatus == LEADER &&
		rf.votedFor == rf.me &&
		rf.checkFollowerCount()
}

func (rf *Raft) checkFollowerCount() bool {
	return rf.followerCount >= len(rf.peers)/2
}

func (rf *Raft) voteMyself() {
	log.Printf("[%v] vote itself\n", rf.me)
	rf.votedFor = rf.me
}

func (rf *Raft) isNotCurrentLead(commingLeadId int) bool {
	return rf.votedFor != -1 && rf.votedFor != commingLeadId
}
