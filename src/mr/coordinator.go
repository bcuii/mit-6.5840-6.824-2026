package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// how long the coordinator waits before assuming a worker has died
// and handing its task to somebody else.
const taskTimeout = 10 * time.Second

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	status    TaskStatus
	startTime time.Time // handed out at; only meaningful while InProgress
	file      string    // map tasks only
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task // one per input file
	reduceTasks []Task // one per reduce bucket
}

// hand out one task, or tell the worker to wait or to exit.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// every map task has to finish before any reduce task can start, since
	// reduce task Y reads mr-0-Y through mr-(nMap-1)-Y.
	switch {
	case !allDone(c.mapTasks):
		if i := claim(c.mapTasks); i >= 0 {
			reply.Type = MapTask
			reply.TaskID = i
			reply.FileName = c.mapTasks[i].file
			reply.NReduce = len(c.reduceTasks)
		} else {
			reply.Type = WaitTask
		}
	case !allDone(c.reduceTasks):
		if i := claim(c.reduceTasks); i >= 0 {
			reply.Type = ReduceTask
			reply.TaskID = i
			reply.NMap = len(c.mapTasks)
		} else {
			reply.Type = WaitTask
		}
	default:
		reply.Type = ExitTask
	}
	return nil
}

// a worker finished a task.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// this can arrive from a worker we already gave up on, long after somebody
	// else redid the task. marking it finished again is harmless.
	tasks := c.mapTasks
	if args.Type == ReduceTask {
		tasks = c.reduceTasks
	}
	if args.TaskID >= 0 && args.TaskID < len(tasks) {
		tasks[args.TaskID].status = Completed
	}
	return nil
}

// claim hands out the first task nobody is working on, taking back along the
// way any whose worker has gone quiet for too long. it returns -1 if every
// task is either finished or with a worker we still believe in.
func claim(tasks []Task) int {
	now := time.Now()
	for i := range tasks {
		t := &tasks[i]
		if t.status == Completed {
			continue
		}
		if t.status == Idle || now.Sub(t.startTime) > taskTimeout {
			t.status = InProgress
			t.startTime = now
			return i
		}
	}
	return -1
}

func allDone(tasks []Task) bool {
	for i := range tasks {
		if tasks[i].status != Completed {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return allDone(c.mapTasks) && allDone(c.reduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}
	for i, f := range files {
		c.mapTasks[i].file = f
	}

	c.server(sockname)
	return &c
}
