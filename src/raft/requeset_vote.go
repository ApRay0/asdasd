package raft

import "time"

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Requester %d :candidate Term : %d . Receiver %d : term : %d", args.CandidateID, args.Term, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
		rf.voteFor = -1
	}
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) &&
		args.LastLogIndex >= len(rf.log) &&
		args.LastLogTerm >= rf.log[len(rf.log)-1].Term {
		DPrintf("%d Voted for %d", rf.me, args.CandidateID)
		rf.refreshElectionTime()
		reply.VoteGranted = true
		rf.voteFor = args.CandidateID
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if time.Now().Before(rf.nextElectionTime) || rf.role == Leader {
		rf.mu.Unlock()
		return
	}
	rf.refreshElectionTime()
	rf.currentTerm++
	DPrintf("------------%d startElection at : %d, term : %d", rf.me, time.Now().UnixMilli(), rf.currentTerm)
	rf.changeRole(Candidate)
	rf.voteFor = rf.me
	requestVoteArgs := RequestVoteArgs{}
	requestVoteArgs.LastLogIndex = len(rf.log)
	requestVoteArgs.Term = rf.currentTerm
	requestVoteArgs.CandidateID = rf.me
	requestVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.votedGatherd = 1
	n := len(rf.peers)
	rf.mu.Unlock()
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(index, &requestVoteArgs, &requestVoteReply)
			if !ok {
				DPrintf("sendRequestVote failed")
				return
			}
			rf.mu.Lock()
			if requestVoteReply.Term > rf.currentTerm {
				rf.currentTerm = requestVoteReply.Term
				rf.changeRole(Follower)
				rf.voteFor = -1
				rf.mu.Unlock()
				return
			}
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			if requestVoteReply.VoteGranted {
				rf.votedGatherd++
				DPrintf("%d Gatherd Vote count : %d, term : %d", rf.me, rf.votedGatherd, rf.currentTerm)
				if rf.votedGatherd >= (n+1)/2 {
					rf.changeRole(Leader)
					for k := 0; k < n; k++ {
						rf.nextIndex[k] = len(rf.log)
						rf.matchIndex[k] = 0
					}
					DPrintf("%d ============== become learder at term : %d", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					rf.sendHeartBeat()
					return
				}
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}(i)
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
