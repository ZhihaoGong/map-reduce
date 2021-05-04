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
	nextWorkerID		int
	workerCol           map[int]*WorkerInfo
	workerColLock		sync.Mutex
	// taskCol             map[int]task

	mapTasks             map[int]*TaskInfo
	reduceTasks          map[int]*TaskInfo
	taskToWorkerMapping		map[int]int

	nReduce				int
	// InitedTasks			map[int]*TaskInfo 
	// MappingTasks		map[int]*TaskInfo 
	// MappedTasks			map[int]*TaskInfo 
	// ReducingTasks		map[int]*TaskInfo
	// ReducedTasks		map[int]*TaskInfo 

}

//
// ApplyForTask allocates a map/reduce task to worker
//
func (c *Coordinator) ApplyForTask(request *TaskApplyReq, reply *TaskApplyRes) error {
	workerID := request.WorkerID
	c.renewHeartBeat(workerID, reply)

	// reply.TaskId = 100
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
				if worker.lastHeartBeart + 10 < time.Now().Unix() {
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
		c.workerCol[workerID] = &WorkerInfo{
			id: workerID,
			status: IdleWorker,
			lastHeartBeart: time.Now().Unix(),
		}
	}

	worker := c.workerCol[workerID]
	worker.lastHeartBeart = time.Now().Unix()
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
	c := Coordinator{
		nReduce: nReduce,
	}

	// Your code here.
	// initalization
	c.workerCol = make(map[int]*WorkerInfo)

	// c.InitedTasks = make(map[int]*TaskInfo)
	// c.MappingTasks = make(map[int]*TaskInfo)
	// c.MappedTasks = make(map[int]*TaskInfo)
	// c.ReducingTasks = make(map[int]*TaskInfo)
	// c.ReducedTasks = make(map[int]*TaskInfo)

	c.workerColLock = sync.Mutex{}

	c.mapTasks = make(map[int]*TaskInfo)
	c.reduceTasks = make(map[int]*TaskInfo)

	// create map tasks
	for index, filename := range files {
		c.mapTasks[index] = &TaskInfo{
			Id: index,
			FileLocations: []string{filename},
			Status: Pending,
			TaskType: Map,
		}
	}

	// create reduce tasks
	for i := 1; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskInfo{
			Id: i,
			FileLocations: []string{},
			Status: Pending,
			TaskType: Reduce,
		}
	}

	c.heartBeatCheck()
	
	fmt.Println("111111111111", c.mapTasks, c.reduceTasks)


	c.server()
	return &c
}
