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
		rf.sendHeartBeat()
		time.Sleep(rf.getSendHeartBeatTimeoutDuration())
	}
}
