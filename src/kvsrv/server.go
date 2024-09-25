package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu             sync.Mutex
	data           map[string]string
	requestResults map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Get start: %s, rqindex : %d", args.Key, args.RequestIndex)
	defer DPrintf("Get end: %s, rqindex : %d", args.Key, args.RequestIndex)

	res, ok := kv.requestResults[args.RequestIndex]
	if ok {
		reply.Value = res
		DPrintf("Get found res")
		return
	}
	value, ok := kv.data[args.Key]

	if ok {
		reply.Value = value
	} else {
		DPrintf("Get key not exits")
	}

	kv.requestResults[args.RequestIndex] = reply.Value

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Put start :  RequestIndex : %d", args.RequestIndex)
	defer DPrintf("Put end : RequestIndex : %d", args.RequestIndex)

	_, ok := kv.requestResults[args.RequestIndex]
	if ok {
		DPrintf("Put found res")
		return
	} else {
		kv.requestResults[args.RequestIndex] = ""
	}

	kv.data[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Append start Key: %s, rqindex : %d", args.Key, args.RequestIndex)
	defer DPrintf("Append end Key: %s, rqindex : %d", args.Key, args.RequestIndex)

	res, ok := kv.requestResults[args.RequestIndex]

	if ok {
		reply.Value = res
		DPrintf("Append found res")
		return
	}

	value, ok := kv.data[args.Key]
	if ok {
		reply.Value = value
	} else {
		kv.data[args.Key] = ""
		reply.Value = ""
		DPrintf("Append key not exits")
	}
	kv.data[args.Key] += args.Value
	kv.requestResults[args.RequestIndex] = reply.Value
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Ack start ClientIndex: %d, ", args.RequestIndex)
	defer DPrintf("Ack end ClientIndex: %d", args.RequestIndex)
	delete(kv.requestResults, args.RequestIndex)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.requestResults = make(map[int64]string)
	return kv
}
