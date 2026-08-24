package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// what the coordinator wants a worker to do next.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // nothing to hand out right now, but the job isn't finished
	ExitTask // every task is done; the worker should exit
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Type     TaskType
	TaskID   int
	FileName string // map only: the input file to read
	NReduce  int    // map only: how many buckets to partition into
	NMap     int    // reduce only: how many intermediate files to read
}

type ReportTaskArgs struct {
	Type   TaskType
	TaskID int
}

type ReportTaskReply struct {
}
