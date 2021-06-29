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

//
// 这个函数主要就是改变一下持久化状态，具体的处理一般在rpc里面就处理了
//
func (rf *Raft) RunServer() {
	log.Printf("RunServer...\nInit Status: %+v", rf)
	for {
		if rf.killed() {
			log.Printf("[%v] was been killed", rf.me)
			break
		}

		// log.Printf(">>> Current raft server[%+v]: %+v", rf.me, rf)
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
				rf.serverStatus = LEADER
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

//
// ask all of server except me
//
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

//
// ask all of server except me
// 每次发headbeat都会发给所有人，然后统计结果，也许直接就不成立了，这时怎么办？其他server接不到heartbeat开始起义，
// 这个仍然是leader，但是已经不起作用了，**不能真正的修改它的手下**（这句话很关键，此时你还是leader吗？）
// 1. server的状态还是leader
// 2. 但是已经不是majority的leader，可以发heartbeat（维持统治），但是无法持久化写进去（执行持久化） --- 这个逻辑怎么处理？
//   2.1 每次发心跳，更新 voteCount，能收回来多少。那么在rpc里面检查？
//		2.1.1 没写进去follower
//		2.1.2 写进去follower，但回来时的信号断了（这个可以由rpc(tcp)解决吧）
// 3. 最终表现的结果是网络分区也能进行执行，只是那个小块被“废弃”了
//
// func (rf *Raft) askHeartbeatToAllPeer(withEntries bool, data ...interface{}) bool {
func (rf *Raft) askHeartbeatToAllPeer() {
	log.Printf("[%v] askHeartbeatToAllPeer start\n", rf.me)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		var reply AppendEntriesReply
		go rf.askHeartbeatToSinglePeer(index, &args, &reply)
	}
}

func (rf *Raft) askHeartbeatToSinglePeer(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		if reply.Success {
			// TODO handle reply.Term ?
			rf.mu.Lock()
			log.Printf("[%+v] askHeartbeatToSinglePeer success: %+v", rf.me, rf)
			rf.voteCount += 1
			rf.mu.Unlock()
		} else {
			// TODO 小term，打回原型
			if reply.Term > rf.currentTerm {
				log.Printf("leader/candidate[%v]'s term[%v] is outdated, new term [%+v], back to follower. %+v", rf.me, rf.currentTerm, reply.Term, rf)
				rf.backToFollower(reply.Term)
			}
		}
	} else {
		log.Printf("[%+v] askHeartbeatToSinglePeer failed, [%+v] partitioned.: %+v", rf.me, server, rf)
	}
}
