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

type JobStatus int

const (
	JobStatusMapping JobStatus = iota
	JobStatusReducing
	JobStatusDone
)


//
// Coordinator manages map/reduce tasks and workers
//
type Coordinator struct {
	nextWorkerID		int
	workerCol           map[int]*WorkerInfo
	workerColLock		sync.RWMutex

	mapTasks            map[int]*TaskInfo
	reduceTasks         map[int]*TaskInfo
	// taskToWorkerMapping	map[int]int

	taskScheduler		Scheduler
	jobStatus			JobStatus
	nReduce				int
}

//
// ApplyForTask allocates a map/reduce task to worker
//
func (c *Coordinator) ApplyForTask(request *TaskApplyReq, reply *TaskApplyRes) error {
	c.workerColLock.Lock()
	defer c.workerColLock.Unlock()
	workerID := c.renewHeartBeat(request.WorkerId, reply)
	worker := c.workerCol[workerID]
	worker.status = IdleWorker
	switch c.jobStatus {
	case JobStatusMapping:	
		taskId, err := c.taskScheduler.Schedule(worker, c.mapTasks)
		if err != nil {
			reply.Task = EmptyTaskData
			return nil
		}
		reply.Task = c.mapTasks[taskId].GetData()
		// c.taskToWorkerMapping[taskId] = workerID
		worker.curTaskId = taskId
		worker.status = MapWorker
		return nil
	case JobStatusReducing:
		// reply.TaskType = ReduceTask
		return nil 
	case JobStatusDone:
		reply.Task = DoneTaskData
		return nil 
	default:
		fmt.Println("Unkown Job Status %d", c.jobStatus)
	}
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
	_heartBeatCheck := func() {
		expireWorkers := []int{}
		c.workerColLock.Lock()
		defer c.workerColLock.Unlock()
		for workerID, worker := range c.workerCol{
			if worker.lastHeartBeart + 10 < time.Now().Unix() {
				expireWorkers = append(expireWorkers, workerID)
			}
		}

		if len(expireWorkers) > 0 {
			fmt.Println("eeeee", expireWorkers)
		}

		for _, workerID := range expireWorkers {
			c.unRegisterWorker(workerID)
		}

		fmt.Println("heartbeatcheck", c.workerCol, int(time.Now().Unix()))
	}

	for {
		_heartBeatCheck()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) renewHeartBeat(workerID int, reply *TaskApplyRes) int {
	if _, ok := c.workerCol[workerID]; !ok {
		// a new worker
		workerID = c.registerWorker()
	}
	reply.WorkerId = workerID

	worker := c.workerCol[workerID]
	worker.lastHeartBeart = time.Now().Unix()
	fmt.Println("22222")
	for k, v := range c.workerCol {
		fmt.Println(k, *v)
	}
	return workerID 
}

//
// worker related
//
func (c *Coordinator) registerWorker() int {
	workerID = c.getNextWorkerID()
	c.workerCol[workerID] = &WorkerInfo{
		id: workerID,
		status: IdleWorker,
		lastHeartBeart: time.Now().Unix(),
		curTaskId: 0,
	}
	return workerID
}

func (c *Coordinator) unRegisterWorker(workerID int) {
	worker := c.workerCol[workerID]
	delete(c.workerCol, workerID)
	if worker.status == MapWorker {
		task := c.mapTasks[worker.curTaskId]
		task.mutex.Lock()
		task.status = Pending
		task.mutex.Unlock()
	} else if worker.status == ReduceWorker {
		task := c.reduceTasks[worker.curTaskId]
		task.mutex.Lock()
		task.status = Pending
		task.mutex.Unlock()
	}
}

//
// Done is used to indicate if the entire job has finished
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// job is done and all workers are stopped
	ret = (c.jobStatus == JobStatusDone && len(c.workerCol) == 0)
	return ret
}

//
// MakeCoordinator create a Coordinator
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		jobStatus: JobStatusMapping,
		taskScheduler: SimpleScheduler{},
	}

	// Your code here.
	// initalization
	c.workerCol = make(map[int]*WorkerInfo)
	c.workerColLock = sync.RWMutex{}

	c.mapTasks = make(map[int]*TaskInfo)
	c.reduceTasks = make(map[int]*TaskInfo)

	// create map tasks
	for index, filename := range files {
		c.mapTasks[index] = &TaskInfo{
			id: index,
			fileLocations: []string{filename},
			status: Pending,
			taskType: MapTask,
		}
	}

	// create reduce tasks
	for i := 1; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskInfo{
			id: i,
			fileLocations: []string{},
			status: Pending,
			taskType: ReduceTask,
		}
	}

	go c.heartBeatCheck()
	
	fmt.Println("111111111111", c.mapTasks, c.reduceTasks)

	c.server()
	return &c
}
