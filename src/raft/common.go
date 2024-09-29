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

func (rf *Raft) getApplyTimeoutDuration() time.Duration {
	ms := 500 + (rand.Int63() % 500)
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

func (rf *Raft) findMaxMatchIndex() int {
	res := rf.lastApplied
	n := len(rf.peers)
	for res < len(rf.log) {
		tmpCount := 1
		for i, matchIndex := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if matchIndex >= res {
				tmpCount++
			}
		}
		if tmpCount < (n+1)/2 {
			break
		}
		res++
	}
	// DPrintf("-=-=-=-=-=-= FindMaxMatchIndex results : %d", res-1)
	return res - 1
}

func (rf *Raft) Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
func (rf *Raft) Max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}
