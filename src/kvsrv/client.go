package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	server           *labrpc.ClientEnd
	index            int64
	nextRequestIndex atomic.Int64
	ack              atomic.Int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.index = nrand()
	ck.nextRequestIndex = atomic.Int64{}
	ck.nextRequestIndex.Store(-1)
	ck.ack = atomic.Int64{}
	ck.ack.Store(-1)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	getArgs := GetArgs{
		Key:          key,
		ClientIndex:  ck.index,
		RequestIndex: ck.nextRequestIndex.Add(1),
		ACK:          ck.ack.Load(),
	}
	getReply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &getArgs, &getReply)
	for !ok {
		DPrintf("GetRPC Failed")
		ok = ck.server.Call("KVServer.Get", &getArgs, &getReply)
	}
	ck.ack.Add(1)
	return getReply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{
		Key:          key,
		Value:        value,
		ClientIndex:  ck.index,
		RequestIndex: ck.nextRequestIndex.Add(1),
		ACK:          ck.ack.Load(),
	}
	putAppendReply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
	for !ok {
		DPrintf("PutAppend Failed")
		ok = ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
	}
	ck.ack.Add(1)
	return putAppendReply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
