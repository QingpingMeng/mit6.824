package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobStatus struct {
	StartTime int64
	Status    string
}

type Coordinator struct {
	// Your definitions here.
	workerStatus      map[int]string
	mapStatus         map[string]JobStatus
	mapTaskNumber     int
	reduceStatus      map[int]JobStatus
	nReducer          int
	intermediateFiles map[int][]string
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

	if !c.AllMapJobDone() {
		reply.MapJob = mapJob
		reply.Done = false
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
	c.mapStatus[args.InputFile] = JobStatus{StartTime: -1, Status: "completed"}
	c.workerStatus[args.Pid] = "idle"
	for r := 0; r < c.nReducer; r++ {
		c.intermediateFiles[r] = append(c.intermediateFiles[r], args.IntermediateFile[r])
	}

	return nil
}

func (c *Coordinator) AllMapJobDone() bool {
	ret := true

	for _, v := range c.mapStatus {
		if v.Status != "completed" {
			ret = false
		}
	}

	return ret
}

func (c *Coordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.ReduceNumber] = JobStatus{StartTime: -1, Status: "completed"}
	c.workerStatus[args.Pid] = "idle"
	return nil
}

func (c *Coordinator) PickMapJob() *MapJob {
	var job *MapJob = nil
	for k, v := range c.mapStatus {
		if v.Status == "pending" {
			job = &MapJob{}
			job.InputFile = k
			job.MapJobNumber = c.mapTaskNumber
			job.ReducerCount = c.nReducer
			c.mapStatus[k] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}
			c.mapTaskNumber++
			break
		}
	}

	return job
}

func (c *Coordinator) PickReduceJob() *ReduceJob {
	var job *ReduceJob = nil
	reducer := -1
	for i, v := range c.reduceStatus {
		if v.Status == "pending" {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	job = &ReduceJob{}
	job.ReduceNumber = reducer
	job.IntermediateFiles = c.intermediateFiles[reducer]

	c.reduceStatus[reducer] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}

	return job
}

func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

func (c *Coordinator) CheckDeadWorker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.mapStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.mapStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}

	for k, v := range c.reduceStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.reduceStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}
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
		if v.Status != "completed" {
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

	c.mapTaskNumber = 0
	c.mapStatus = make(map[string]JobStatus)
	for _, v := range files {
		c.mapStatus[v] = JobStatus{StartTime: -1, Status: "pending"}
	}

	c.nReducer = nReduce

	c.reduceStatus = make(map[int]JobStatus)
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = JobStatus{StartTime: -1, Status: "pending"}
	}

	c.workerStatus = make(map[int]string)
	c.intermediateFiles = make(map[int][]string)
	c.StartTicker()
	c.server()
	return &c
}
