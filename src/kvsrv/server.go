package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu               sync.Mutex
	data             map[string]string
	nextRequestIndex map[int64]int64
	requestResults   map[int64]map[int64]string
	lastAck          map[int64]int64
}

func (kv *KVServer) CheckNextRequest(clientIndex int64, requestIndex int64) bool {
	nextRequestIndex, ok := kv.nextRequestIndex[clientIndex]
	if !ok {
		kv.nextRequestIndex[clientIndex] = 0
		if requestIndex != 0 {
			return false
		}
	} else {
		if requestIndex != nextRequestIndex {
			return false
		}
	}
	return true
}

func (kv *KVServer) TruncateData(currentAck int64, index int64) {
	_, ok := kv.lastAck[index]
	if !ok {
		kv.lastAck[index] = -1
	}
	lastAck := kv.lastAck[index]
	DPrintf("TruncateData begin : lastAck : %d currentAck : %d,  size : %d", lastAck, currentAck, len(kv.requestResults[index]))
	for i := lastAck + 1; i <= currentAck; i++ {
		kv.requestResults[index][i] = ""
		delete(kv.requestResults[index], i)
	}
	kv.lastAck[index] = currentAck
	DPrintf("TruncateData end: lastAck : %d currentAck : %d, size : %d", lastAck, currentAck, len(kv.requestResults[index]))
}

func (kv *KVServer) TryFindResult(clientIndex int64, requestIndex int64) (string, bool) {
	_, ok := kv.requestResults[clientIndex]
	res := ""
	if !ok {
		kv.requestResults[clientIndex] = make(map[int64]string)
	}
	// resIndex := requestIndex - kv.lastAck[clientIndex] - 1
	// if resIndex >= int64(len(kv.requestResults[clientIndex])) {
	// 	return res, false
	// }
	// res = kv.requestResults[clientIndex][resIndex]
	res, ok = kv.requestResults[clientIndex][requestIndex]
	return res, ok
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer DPrintf("Get end: %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	defer kv.mu.Unlock()
	DPrintf("Get start: %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	kv.TruncateData(args.ACK, args.ClientIndex)

	res, ok := kv.TryFindResult(args.ClientIndex, args.RequestIndex)
	if ok {
		reply.Value = res
		DPrintf("Get found res")
		return
	}

	if !kv.CheckNextRequest(args.ClientIndex, args.RequestIndex) {
		return
	}

	value, ok := kv.data[args.Key]
	if ok {
		reply.Value = value
	} else {
		DPrintf("Get key not exits")
	}
	kv.requestResults[args.ClientIndex][args.RequestIndex] = reply.Value
	kv.nextRequestIndex[args.ClientIndex]++
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer DPrintf("Put end: %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	defer kv.mu.Unlock()
	DPrintf("Put start index : %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	kv.TruncateData(args.ACK, args.ClientIndex)

	res, ok := kv.TryFindResult(args.ClientIndex, args.RequestIndex)
	if ok {
		reply.Value = res
		DPrintf("Put found res")
		return
	}

	if !kv.CheckNextRequest(args.ClientIndex, args.RequestIndex) {
		return
	}

	kv.data[args.Key] = args.Value
	kv.requestResults[args.ClientIndex][args.RequestIndex] = ""
	kv.nextRequestIndex[args.ClientIndex]++
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer DPrintf("Append end: %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	defer kv.mu.Unlock()
	DPrintf("Append start: %d, rqindex : %d", args.ClientIndex, args.RequestIndex)
	kv.TruncateData(args.ACK, args.ClientIndex)

	res, ok := kv.TryFindResult(args.ClientIndex, args.RequestIndex)
	if ok {
		reply.Value = res
		DPrintf("Append found res")
		return
	}

	if !kv.CheckNextRequest(args.ClientIndex, args.RequestIndex) {
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
	kv.requestResults[args.ClientIndex][args.RequestIndex] = reply.Value
	kv.nextRequestIndex[args.ClientIndex]++
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.nextRequestIndex = make(map[int64]int64)
	kv.requestResults = make(map[int64]map[int64]string)
	kv.lastAck = make(map[int64]int64)
	return kv
}
