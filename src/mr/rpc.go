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

type GetInputPathArgs struct {
	Index int
}

type GetInputPathReply struct {
	InputPath string
}

// 获取map task
type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	Task MapTask
}

// 获取reduce task
type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	Task ReduceTask
}

type MapWorkerDoneArgs struct {
	Id int
}

type MapWorkerDoneReply struct {
	Isok bool
}

type ReduceWorkerDoneArgs struct {
	Id int
}

type ReduceWorkerDoneReply struct {
	Isok bool
}

type AllMapDoneArgs struct {
}

type AllMapDoneReply struct {
	Isok bool
}

type AllReduceDoneArgs struct {
}

type AllReduceDoneReply struct {
	Isok bool
}

type GetnReduceArgs struct {
}

type GetnReduceReply struct {
	NumReduce int
}

type GetnMapArgs struct {
}

type GetnMapReply struct {
	NumMap int
}

type GetMapDoneCountArgs struct {
}

type GetMapDoneCountReply struct {
	Num int
}

type GetReduceDoneCountArgs struct {
}

type GetReduceDoneCountReply struct {
	Num int
}

type GetMapIsAllDoneArgs struct {
}

type GetMapIsAllDoneReply struct {
	IsDone bool
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
