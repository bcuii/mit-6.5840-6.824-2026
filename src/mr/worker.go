package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "bytes"
import "encoding/json"
import "io"
import "sort"
import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &GetTaskArgs{}, &reply) {
			// the coordinator is unreachable, so there is nothing left to do.
			return
		}

		switch reply.Type {
		case MapTask:
			doMap(&reply, mapf)
			report(MapTask, reply.TaskID)
		case ReduceTask:
			doReduce(&reply, reducef)
			report(ReduceTask, reply.TaskID)
		case WaitTask:
			// other workers are still busy; ask again shortly. exiting here
			// would risk leaving the job unfinished.
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
}

// run one map task: read the input file, hand it to mapf, and partition the
// results into NReduce intermediate files named mr-<map task>-<bucket>.
func doMap(t *GetTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(t.FileName)
	if err != nil {
		log.Fatalf("cannot read %v: %v", t.FileName, err)
	}

	buckets := make([][]KeyValue, t.NReduce)
	for _, kv := range mapf(t.FileName, string(content)) {
		y := ihash(kv.Key) % t.NReduce
		buckets[y] = append(buckets[y], kv)
	}

	// every bucket gets a file, even an empty one, so that the reduce tasks
	// can open all nMap of them unconditionally.
	for y, bucket := range buckets {
		name := fmt.Sprintf("mr-%d-%d", t.TaskID, y)
		writeFileAtomically(name, func(f *os.File) {
			enc := json.NewEncoder(f)
			for _, kv := range bucket {
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("cannot encode into %v: %v", name, err)
				}
			}
		})
	}
}

// run one reduce task: read this bucket from every map task's output, group
// the pairs by key, and write the results to mr-out-<reduce task>.
func doReduce(t *GetTaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for x := 0; x < t.NMap; x++ {
		name := fmt.Sprintf("mr-%d-%d", x, t.TaskID)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open %v: %v", name, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("cannot decode %v: %v", name, err)
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// sorting puts equal keys next to each other, so one linear pass can
	// collect each key's values.
	sort.Sort(ByKey(kva))

	// build the whole output before opening a file. reducef is application
	// code that may block for a long time or exit outright, and we would
	// rather not be holding a half-written temporary file while it runs.
	var buf bytes.Buffer
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(&buf, "%v %v\n", kva[i].Key, output)

		i = j
	}

	name := fmt.Sprintf("mr-out-%d", t.TaskID)
	writeFileAtomically(name, func(f *os.File) {
		if _, err := f.Write(buf.Bytes()); err != nil {
			log.Fatalf("cannot write %v: %v", name, err)
		}
	})
}

// build the file in a temporary one and rename it into place, so that nobody
// ever sees a half-written file: this worker may crash partway through, and
// another worker may be running the same task at the same time.
func writeFileAtomically(name string, write func(*os.File)) {
	f, err := os.CreateTemp(".", "mrtmp-")
	if err != nil {
		log.Fatalf("cannot create a temp file for %v: %v", name, err)
	}
	write(f)
	if err := f.Close(); err != nil {
		log.Fatalf("cannot close %v: %v", f.Name(), err)
	}
	if err := os.Rename(f.Name(), name); err != nil {
		log.Fatalf("cannot rename %v to %v: %v", f.Name(), name, err)
	}
}

// tell the coordinator that a task is finished.
func report(t TaskType, id int) {
	args := ReportTaskArgs{Type: t, TaskID: id}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
