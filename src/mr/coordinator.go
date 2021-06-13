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
	if !c.workerCol.HasWorker(workerId) {
		// Update workerId to a valid one
		workerId = c.workerCol.GetNextWorkerId()
		c.workerCol.RegisterWorker(workerId)
	} else {
		c.workerCol.RenewHeartBeat(workerId)
	}

	reply.WorkerId = workerId

	// _, _  := c.workerCol.GetWorker(workerId)
	// reply.
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

func (c *Coordinator) CleanDisconnectedWorker() {
	for {
		c.workerCol.CleanDisconnectedWorker()
		time.Sleep(time.Second)
	}
}

//
// MakeCoordinator create a Coordinator
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskToWorkerMapping: make(map[int]int),
		scheduler:           RandomScheduler{},
	}

	c.workerCol = WorkerCol{
		workers:               make(map[int]WorkerInfo),
		disconnestedThedshold: 10,
	}

	c.taskCol = TaskCol{
		Pending: make(map[int]TaskInfo),
		Map:     make(map[int]TaskInfo),
		Reduce:  make(map[int]TaskInfo),
		Done:    make(map[int]TaskInfo),
	}

	go c.CleanDisconnectedWorker()

	c.server()
	return &c
}
