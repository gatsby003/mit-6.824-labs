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
type TaskRequest struct {
	Ready bool
}

type TaskResult struct {
	Success            bool
	Intermediate_Files []string
	IsMap              bool
	IsReduce           bool
}

type Response struct {
	TaskAlloted        bool
	Filename           string
	Is_Map             bool
	Is_Reduce          bool
	N_Reduce           int
	Map_Task_Number    int
	Reduce_Task_Number int
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
