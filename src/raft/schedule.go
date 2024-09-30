package raft

import "time"

func (rf *Raft) electionSchedule() {
	for !rf.killed() {
		rf.startElection()
		time.Sleep(rf.getShortTime())
	}
}

func (rf *Raft) heartBeatSchedule() {
	for !rf.killed() {
		rf.startSendAppendEntries(true)
		time.Sleep(rf.getSendHeartBeatTimeoutDuration())
	}
}

func (rf *Raft) applySchedule() {
	for !rf.killed() {
		rf.startApply()
		time.Sleep(rf.getApplyTimeoutDuration())
	}
}

func (rf *Raft) appendEntriesSchedule() {
	for !rf.killed() {
		rf.startSendAppendEntries(false)
		time.Sleep(rf.getAppendEntriesTimeoutDuration())
	}
}
