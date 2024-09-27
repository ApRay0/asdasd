package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	ms := 150 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) getSendHeartBeatTimeoutDuration() time.Duration {
	ms := 100 + (rand.Int63() % 50)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) getShortTime() time.Duration {
	ms := 10
	return time.Duration(ms) * time.Millisecond
}
func (rf *Raft) refreshElectionTime() {
	rf.nextElectionTime = time.Now().Add(rf.getElectionTimeoutDuration())
	DPrintf("%d refreshElectionTime to %d: ", rf.me, rf.nextElectionTime.UnixMilli())
}

func (rf *Raft) changeRole(role int) {
	switch role {
	case Leader:
		DPrintf("----------%d change role to LEADER at term %d", rf.me, rf.currentTerm)
	case Candidate:
		DPrintf("----------%d change role to Candidate at term %d", rf.me, rf.currentTerm)
	case Follower:
		DPrintf("----------%d change role to Follower at term %d", rf.me, rf.currentTerm)
	}
	rf.role = role
}

func (rf *Raft) Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
