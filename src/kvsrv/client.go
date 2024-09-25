package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
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
	var requestIndex = nrand()
	getArgs := GetArgs{
		Key:          key,
		RequestIndex: requestIndex,
	}
	getReply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &getArgs, &getReply)
	for !ok {
		DPrintf("GetRPC Failed")
		ok = ck.server.Call("KVServer.Get", &getArgs, &getReply)
	}
	ackArgs := AckArgs{
		RequestIndex: requestIndex,
	}
	ackReply := AckReply{}
	ok = ck.server.Call("KVServer.Ack", &ackArgs, &ackReply)
	for !ok {
		DPrintf("AckRPC Failed")
		ok = ck.server.Call("KVServer.Ack", &ackArgs, &ackReply)
	}
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
	requestIndex := nrand()

	putAppendArgs := PutAppendArgs{
		Key:          key,
		Value:        value,
		RequestIndex: requestIndex,
	}
	putAppendReply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
	for !ok {
		DPrintf("PutAppend Failed")
		ok = ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
	}
	ackArgs := AckArgs{
		RequestIndex: requestIndex,
	}
	ackReply := AckReply{}
	ok = ck.server.Call("KVServer.Ack", &ackArgs, &ackReply)
	for !ok {
		DPrintf("AckRPC Failed")
		ok = ck.server.Call("KVServer.Ack", &ackArgs, &ackReply)
	}
	return putAppendReply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
