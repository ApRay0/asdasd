package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	ClientIndex  int64
	RequestIndex int64
	ACK          int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key          string
	ClientIndex  int64
	RequestIndex int64
	ACK          int64
}

type GetReply struct {
	Value string
}
