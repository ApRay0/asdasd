package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	RequestIndex int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key          string
	RequestIndex int64
}

type GetReply struct {
	Value string
}

type AckArgs struct {
	RequestIndex int64
}

type AckReply struct {
}
