package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

//
// Coordinator manages map/reduce tasks and workers
//
type Coordinator struct {
	workerCol           map[int]WorkerInfo
	taskCol             map[int]TaskInfo
	taskToWorkerMapping map[int]int
}

//
// ApplyForTask allocates a map/reduce task to worker
//
func (c *Coordinator) ApplyForTask(request *TaskApplyReq, reply *TaskApplyRes) error {
	workerID := request.WorkerID

	if _, ok := c.workerCol[workerID]; !ok {
		c.workerCol[workerID] = WorkerInfo{
			id:             workerID,
			status:         IdleWorker.String(),
			lastHeartBeart: time.Now().Unix(),
		}
	}

	// reply.TaskId = 100
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// Done is used to indicate if the entire job has finished
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	return ret
}

//
// MakeCoordinator create a Coordinator
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
