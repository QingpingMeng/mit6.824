package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	workerStatus      map[int]string
	mapStatus         map[string]string
	reduceStatus      map[int]bool
	nReducer          int
	intermediateFiles []string
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	// register worker
	if c.workerStatus[args.Pid] == "" {
		c.workerStatus[args.Pid] = "idle"
	}

	mapJob := c.PickMapJob()
	if mapJob != nil {
		reply.MapJob = mapJob
		reply.Done = false
		c.workerStatus[args.Pid] = "busy"
		c.mu.Unlock()
		return nil
	}

	reduceJob := c.PickReduceJob()
	if reduceJob != nil {
		reply.ReduceJob = reduceJob
		reply.Done = false
		c.workerStatus[args.Pid] = "busy"
		c.mu.Unlock()
		return nil
	}

	c.mu.Unlock()
	reply.Done = c.Done()
	return nil
}

func (c *Coordinator) ReportMapTask(args *ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapStatus[args.InputFile] = "completed"
	c.workerStatus[args.Pid] = "idle"
	c.intermediateFiles = append(c.intermediateFiles, args.IntermediateFile)

	return nil
}

func (c *Coordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.ReduceNumber] = true
	c.workerStatus[args.Pid] = "idle"
	return nil
}

func (c *Coordinator) PickMapJob() *MapJob {
	var job *MapJob = nil
	for k, v := range c.mapStatus {
		if v == "pending" {
			job = &MapJob{}
			job.InputFile = k
			job.ReducerCount = c.nReducer
			c.mapStatus[k] = "running"
			break
		}
	}

	return job
}

func (c *Coordinator) PickReduceJob() *ReduceJob {
	var job *ReduceJob = nil
	reducer := -1
	for i, v := range c.reduceStatus {
		if !v {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	job = &ReduceJob{}
	job.ReduceNumber = reducer
	job.IntermediateFiles = c.intermediateFiles

	return job
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := true

	for _, v := range c.reduceStatus {
		if !v {
			return false
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapStatus = make(map[string]string)
	for _, v := range files {
		c.mapStatus[v] = "pending"
	}

	c.nReducer = nReduce

	c.reduceStatus = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = false
	}

	c.workerStatus = make(map[int]string)

	c.server()
	return &c
}
