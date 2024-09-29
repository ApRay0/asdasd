package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// func (rf *Raft) sendHeartBeat() {
// 	rf.mu.Lock()
// 	if rf.role != Leader {
// 		rf.mu.Unlock()
// 		return
// 	}
// 	DPrintf("%d sendHeartBeat term : %d", rf.me, rf.currentTerm)
// 	rf.mu.Unlock()
// 	for i := 0; i < len(rf.peers); i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		go func(i int) {
// 			rf.mu.Lock()
// 			if rf.role != Leader {
// 				DPrintf("%d sendHeartBeat breaked cause not leader term : %d", rf.me, rf.currentTerm)
// 				rf.mu.Unlock()
// 				return
// 			}
// 			args := AppendEntriesArgs{}
// 			args.LeaderId = rf.me
// 			args.Term = rf.currentTerm
// 			args.LeaderCommit = rf.commitIndex
// 			args.PrevLogIndex = rf.nextIndex[i] - 1
// 			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
// 			rf.mu.Unlock()
// 			reply := AppendEntriesReply{}
// 			ok := rf.sendAppendEntries(i, &args, &reply)
// 			if !ok {
// 				DPrintf("sendHeartBeatRPC failed, %d to %d", args.LeaderId, i)
// 				return
// 			}
// 			rf.mu.Lock()
// 			if reply.Term > rf.currentTerm {
// 				rf.currentTerm = reply.Term
// 				rf.changeRole(Follower)
// 				rf.voteFor = -1
// 				rf.mu.Unlock()
// 				return
// 			}
// 			if reply.Success {
// 				rf.nextIndex[i] += len(args.Entries)
// 				rf.mu.Unlock()
// 				return
// 			}
// 			rf.mu.Unlock()
// 		}(i)
// 	}
// }

func (rf *Raft) startSendAppendEntries(isHeartBeat bool) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	DPrintf("Leaderer %d startSendAppendEntries term : %d, log len: %d, isHeartBeat : %+v", rf.me, rf.currentTerm, len(rf.log), isHeartBeat)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.role != Leader {
				DPrintf("%d startSendAppendEntries breaked cause not leader term : %d", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			if !isHeartBeat {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				DPrintf("sendAppendEntriesRPC failed, %d to %d", args.LeaderId, i)
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.changeRole(Follower)
				rf.voteFor = -1
				rf.mu.Unlock()
				return
			}
			if reply.Success && !isHeartBeat {
				rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.commitIndex = rf.findMaxMatchIndex()
				rf.mu.Unlock()
				return
			}
			if !reply.Success {
				DPrintf("%d nextIndex--", i)
				rf.nextIndex[i]--
				rf.mu.Unlock()
				rf.startSendAppendEntries(isHeartBeat)
				return
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d start AppendEntries at term : %d, leader : %d, leaderTerm : %d, LeaderCommit : %d,  PrevLogIndex: % d, entries : %+v",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.Entries)
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.changeRole(Follower)
		rf.voteFor = -1
		rf.currentTerm = args.Term
	}
	rf.refreshElectionTime()
	if args.PrevLogIndex > len(rf.log) {
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[args.PrevLogIndex-1:]
		return
	}
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.Min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Success = true
}

func (rf *Raft) startApply() {
	rf.mu.Lock()
	DPrintf("%d startApply last apply : %d : current commitIndex : %d", rf.me, rf.lastApplied, rf.commitIndex)
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("%d apply %d", rf.me, i)
		msg := ApplyMsg{}
		msg.Command = rf.log[i].Command
		msg.CommandIndex = i
		msg.CommandValid = true
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
