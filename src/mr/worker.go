package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)


var workerID int = -1

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
		applyTask()
		time.Sleep(time.Second)
	}

}

func applyTask() {
	resquest := TaskApplyReq{
		WorkerID: workerID,
	}
	reply := TaskApplyRes{}

	call("Coordinator.ApplyForTask", &resquest, &reply)
	if workerID == -1 {
		workerID = reply.WorkerID
	}

	// reply.Y should be 100.
	fmt.Printf("reply.taskId %v\n", reply.TaskId)
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
