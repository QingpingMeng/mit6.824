package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	shouldStop := false
	// Your worker implementation here.
	for !shouldStop {
		reply := RequestTask()
		if reply.Done {
			shouldStop = true
			fmt.Println("All jobs done, worker exit...")
			continue
		}

		if reply.MapJob != nil { // map
			HandleMapJob(reply.MapJob, mapf)
		}

		if reply.ReduceJob != nil { // reduce
			HandleReduceJob(reply.ReduceJob, reducef)
		}

		// time.Sleep(1 * time.Second)
	}
}

func HandleMapJob(job *MapJob, mapf func(string, string) []KeyValue) {
	filename := job.InputFile
	reduceCount := job.ReducerCount
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))
	intermieateFile := fmt.Sprintf("mr-%v-%v", filename, ihash(filename)%reduceCount)
	ofile, _ := os.Create(intermieateFile)

	enc := json.NewEncoder(ofile)
	for _, kv := range kva {
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Printf("error encoding\n")
		}
	}

	ofile.Close()
	ReportMapTask(ReportMapTaskArgs{InputFile: filename, IntermediateFile: intermieateFile, Pid: os.Getpid()})
}

func HandleReduceJob(job *ReduceJob, reducef func(string, []string) string) {
	files := []string{}

	for _, f := range job.IntermediateFiles {
		if strings.HasSuffix(f, fmt.Sprintf("-%v", job.ReduceNumber)) {
			files = append(files, f)
		}
	}

	if len(files) == 0 {
		ReportReduceTask(ReportReduceTaskArgs{Pid: os.Getpid(), ReduceNumber: job.ReduceNumber})
		return
	}

	intermediate := []KeyValue{}

	for _, f := range files {
		fs, _ := os.Open(f)
		dec := json.NewDecoder(fs)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		fs.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", job.ReduceNumber)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ReportReduceTask(ReportReduceTaskArgs{Pid: os.Getpid(), ReduceNumber: job.ReduceNumber})
}

func RequestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	args.Pid = os.Getpid()

	reply := RequestTaskReply{}

	call("Coordinator.RequestTask", &args, &reply)
	return reply
}

func ReportMapTask(args ReportMapTaskArgs) ReportMapTaskReply {
	reply := ReportMapTaskReply{}
	call("Coordinator.ReportMapTask", &args, &reply)
	return reply
}

func ReportReduceTask(args ReportReduceTaskArgs) ReportReduceTaskReply {
	reply := ReportReduceTaskReply{}
	call("Coordinator.ReportReduceTask", &args, &reply)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
