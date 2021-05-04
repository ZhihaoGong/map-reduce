package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"fmt"
	"sync"
)

//
// Coordinator manages map/reduce tasks and workers
//
type Coordinator struct {
	workerCol           map[int]*worker
	nextWorkerID		int
	// taskCol             map[int]task
	// taskToWorkerMapping map[int]int

	InitedTasks			map[int]task 
	MappingTasks		map[int]task 
	MappedTasks			map[int]task 
	ReducingTasks		map[int]task
	ReducedTasks		map[int]task 

	workerColLock		sync.Mutex
}

type WorkerStatus int

const (
	IdleWorker WorkerStatus = iota
    MapWorker
    ReduceWorker
)


func (ws WorkerStatus) String() string {
    return [...]string{"Idle", "Map", "Reduce"}[ws]
}

type worker struct {
	id            	int
	status        	WorkerStatus
	taskID			int
	lastHeartBeart	int
}


type task struct {
	id     int
	fileLocation string
	
}

//
// ApplyForTask allocates a map/reduce task to worker
//
func (c *Coordinator) ApplyForTask(request *TaskApplyReq, reply *TaskApplyRes) error {
	workerID := request.WorkerID
	c.renewHeartBeat(workerID, reply)

	reply.TaskId = 100
	return nil
}

func (c *Coordinator) getNextWorkerID() int {
	// may overflow
	c.nextWorkerID++
	return c.nextWorkerID
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
// HeartBeat
//
func (c *Coordinator) heartBeatCheck() {
	go func() {
		for {
			expireWorkers := []int{}
			c.workerColLock.Lock()
			for workerID, worker := range c.workerCol{
				if worker.lastHeartBeart + 10 < int(time.Now().Unix()) {
					fmt.Println("eeeee", workerID)
					expireWorkers = append(expireWorkers, workerID)
					fmt.Println("eeeee", expireWorkers)
				}
			}
			for _, workerID := range expireWorkers {
				fmt.Println("eeeee", expireWorkers)
				delete(c.workerCol, workerID)
			}

			fmt.Println("heartbeatcheck", c.workerCol, int(time.Now().Unix()))
			c.workerColLock.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

func (c *Coordinator) renewHeartBeat(workerID int, reply *TaskApplyRes) {
	c.workerColLock.Lock()
	if workerID == -1 {
		// a new worker
		workerID = c.getNextWorkerID()
		reply.WorkerID = workerID
		c.workerCol[workerID] = &worker{
			id: workerID,
			status: IdleWorker,
			lastHeartBeart: int(time.Now().Unix()),
		}
	}

	worker := c.workerCol[workerID]
	worker.lastHeartBeart = int(time.Now().Unix())
	fmt.Println("22222")
	for k, v := range c.workerCol {
		fmt.Println(k, *v)
	}
	c.workerColLock.Unlock()
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
	// initalization
	c.workerCol = make(map[int]*worker)

	c.InitedTasks = make(map[int]task)
	c.MappingTasks = make(map[int]task)
	c.MappedTasks = make(map[int]task)
	c.ReducingTasks = make(map[int]task)
	c.ReducedTasks = make(map[int]task)

	c.workerColLock = sync.Mutex{}

	for index, filename := range 	files {
		c.InitedTasks[index] = task{
			id: index,
			fileLocation: filename,
		}
	}

	c.heartBeatCheck()
	
	fmt.Println("111111111111", c.InitedTasks)


	c.server()
	return &c
}
