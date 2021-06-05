package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

//
// Coordinator manages map/reduce tasks and workers
//
type Coordinator struct {
	workerCol           WorkerCol
	taskCol             TaskCol
	taskToWorkerMapping map[int]int
	scheduler           Scheduler
}

//
// ApplyForTask allocates a map/reduce task to worker
//
func (c *Coordinator) ApplyForTask(request *TaskApplyReq, reply *TaskApplyRes) error {
	workerId := request.WorkerId

	if c.workerCol.HasWorker(workerId) {
		c.workerCol.RegisterWorker(workerId)
	} else {
		// Renew worker heartbeart
		c.workerCol.RenewHeartBeat(workerId)
	}

	// Schedule pending task to idle worker
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
	c := Coordinator{
		workerCol:           WorkerCol{},
		taskCol:             TaskCol{},
		taskToWorkerMapping: make(map[int]int),
	}

	// Your code here.

	c.server()
	return &c
}
