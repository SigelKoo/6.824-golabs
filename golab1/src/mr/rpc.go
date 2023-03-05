package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapNumArgs struct {
}

type MapNumReply struct {
	Num int
}

type MapJobArgs struct {
	Content string
}

type MapJobReply struct {
	File     string
	MapJobID int
}

type MapJobDoneArgs struct {
	TaskType int
	FileName string
}

type MapJobDoneReply struct {
	Done bool
}

type ReduceArgs struct {
	Content string
}

type ReduceReply struct {
	ReduceJobID int
}

type ReduceJobDoneArgs struct {
	JobID int
}

type ReduceJobDoneReply struct {
}

type MROverArgs struct {
}

type MROverReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
