package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

var workerID int = -1
var task TaskData = EmptyTaskData

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	for true {
		fmt.Println("Infinite Loop")
		isDone := applyTask()
		if isDone {
			return
		}
		time.Sleep(time.Second)
	}

}

func applyTask() (isDone bool) {
	isDone = false
	resquest := TaskApplyReq{
		WorkerId: workerID,
		DoneTask: task,
	}
	reply := TaskApplyRes{}

	call("Coordinator.ApplyForTask", &resquest, &reply)
	workerID = reply.WorkerId
	task := reply.Task
	fmt.Printf("reply.taskId %v taskType %v\n", task.Id, task.TaskType)
	
	switch task.TaskType {
	case DoneTask:
		isDone = true 
	case MapTask:
		doMap(&task)
	case ReduceTask:
		doReduce(&task)
	default:
	}
	return 
}

func doMap(task *TaskData) {
	time.Sleep(time.Second*11)
	fmt.Println("doMap ", task.Id, task.TaskType, task.FileLocations)
}

func doReduce(task *TaskData) {
	fmt.Println("doReduce ", task.Id, task.TaskType, task.FileLocations)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// TODO: set rpc timeout
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

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
