package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Job struct {
	Type              int
	WorkerNumber      int
	InputFile         string
	IntermediateFiles []string
	ShouldQuit        bool
}

type MapJob struct {
	InputFile    string
	MapJobNumber int
	ReducerCount int
}

type ReduceJob struct {
	IntermediateFiles []string
	ReduceNumber      int
}

type RequestTaskArgs struct {
	Pid int
}

type RequestTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool
}

type ReportMapTaskArgs struct {
	InputFile        string
	IntermediateFile []string
	Pid              int
}

type ReportMapTaskReply struct {
}

type ReportReduceTaskArgs struct {
	Pid          int
	ReduceNumber int
}

type ReportReduceTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
