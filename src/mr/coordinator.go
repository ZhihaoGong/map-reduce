package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
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
		c.workerCol.RegisterWorker(workerId)
	} else {
		c.workerCol.RenewHeartBeat(workerId)
	}

	worker, err := c.workerCol.GetWorker(workerId)
	if err != nil {
		panic("Workerid " + strconv.Itoa(workerId) + " is not registered.")
	}

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
		c.CleanDisconnectedWorker()
		time.Sleep(time.Second)
	}
}

//
// MakeCoordinator create a Coordinator
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workerCol:           WorkerCol{},
		taskCol:             TaskCol{},
		taskToWorkerMapping: make(map[int]int),
		scheduler:           RandomScheduler{},
	}

	go c.CleanDisconnectedWorker()

	c.server()
	return &c
}
