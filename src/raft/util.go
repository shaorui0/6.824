package raft

import (
	"log"
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
			log.Printf("[%v] was been killed\n", rf.me)
			break
		}

		log.Printf(">>> Current raft server[%+v]: %+v\n", rf.me, rf)

		if rf.serverStatus == FOLLOWER {
			// get rpc(vote, heartbeat) when I sleeped
			// if rf.receivedRequestVote {
			// 	rf.receivedRequestVote = false // reset
			// 	continue
			// }
			// if rf.receivedAppendEntries {
			// 	rf.receivedAppendEntries = false // reset
			// 	continue
			// }

			rf.sleepMicroSecond(rf.voteTimeout)

			// 睡一觉起来，发现处理过rpc，再睡一觉
			if rf.receivedRequestVote {
				rf.receivedRequestVote = false // reset
				continue
			}
			if rf.receivedAppendEntries {
				rf.receivedAppendEntries = false // reset
				continue
			}

			rf.upToCandidate() // try to be candidate

		} else if rf.serverStatus == CANDIDATE {
			rf.askVoteToAllPeer() // 默认rpc很快

			rf.sleepMicroSecond(rf.voteTimeout)

			// check rpc's result: majority
			if rf.checkFollowerCount() && rf.votedFor == rf.me {
				log.Printf("[%v] askVoteToAllPeer success, server: %+v\n", rf.me, rf)
				rf.upToLeader()
			} else {
				// reset election
				rf.upToCandidate()
				rf.voteTimeout = rf.generateNewTimeout("RequestVote")
				log.Printf("[%v] askVoteToAllPeer failed, new election. server: %+v\n", rf.me, rf)
			}
		} else if rf.serverStatus == LEADER {
			// 理论上，leader 永远是 leader，应该是稳定状态了，有什么可能降级吗？网络分区了被降级(本质是 term 过期了)
			// 如果接到了client的请求，就发出heartbeat，
			rf.askHeartbeatToAllPeer()

			rf.sleepMicroSecond(HEARTBEAT)

			if rf.checkFollowerCount() && rf.votedFor == rf.me {
				log.Printf("[%v] askHeartbeatToAllPeer success, server: %+v\n", rf.me, rf)
			} else {
				log.Printf("[%v] askHeartbeatToAllPeer failed, server: %+v\n", rf.me, rf)
				// ...心跳失败继续心跳
			}

			// TODO send append log rpc in the future
		}
	}
}

//
// ask all of server except me
//
func (rf *Raft) askVoteToAllPeer() {
	log.Printf("[%v] askVoteToAllPeer start\n", rf.me)
	rf.followerCount = 0
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		var reply RequestVoteReply
		go rf.askRequestVoteToSinglePeer(index, &args, &reply)
	}
	log.Printf("[%v] askVoteToAllPeer end\n", rf.me)
}

//
// ask all of server except me
// 每次发headbeat都会发给所有人，然后统计结果，也许直接就不成立了，这时怎么办？其他server接不到heartbeat开始起义，
// 这个仍然是leader，但是已经不起作用了，**不能真正的修改它的手下**（这句话很关键，此时你还是leader吗？）
// 1. server的状态还是leader
// 2. 但是已经不是majority的leader，可以发heartbeat（维持统治），但是无法持久化写进去（执行持久化） --- 这个逻辑怎么处理？
//   2.1 每次发心跳，更新 followerCount，能收回来多少。那么在rpc里面检查？
//		2.1.1 没写进去follower
//		2.1.2 写进去follower，但回来时的信号断了（这个可以由rpc(tcp)解决吧）
// 3. 最终表现的结果是网络分区也能进行执行，只是那个小块被“废弃”了
//
// func (rf *Raft) askHeartbeatToAllPeer(withEntries bool, data ...interface{}) bool {
func (rf *Raft) askHeartbeatToAllPeer() {
	log.Printf("[%v] askHeartbeatToAllPeer start\n", rf.me)
	rf.followerCount = 0
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

func (rf *Raft) askRequestVoteToSinglePeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		if reply.VoteGranted {
			// TODO handle reply.Term ?
			rf.mu.Lock()
			rf.followerCount += 1
			log.Printf("[%+v] askRequestVoteToSinglePeer success: %+v", rf.me, rf)
			rf.mu.Unlock()
		} else {
			// TODO 拒绝一般是有原因的，会有哪些原因？很多...未来还会增加更多的原因
			// TODO 小term，打回原型
			if reply.Term > rf.currentTerm {
				log.Printf("leader/candidate[%v]'s term[%v] is outdated, new term [%+v], back to follower. %+v", rf.me, rf.currentTerm, reply.Term, rf)
				rf.backToFollower(reply.Term)
			}
		}
	} else {
		log.Printf("[%+v] askRequestVoteToSinglePeer failed, [%+v] partitioned.: %+v", rf.me, server, rf)
	}
}

func (rf *Raft) askHeartbeatToSinglePeer(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		if reply.Success {
			// TODO handle reply.Term ?
			rf.mu.Lock()
			log.Printf("[%+v] askHeartbeatToSinglePeer success: %+v", rf.me, rf)
			rf.followerCount += 1
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
