package mr

import (
	"sync"
)


type TaskType int

const (
	EmptyTask	TaskType = iota
	MapTask
	ReduceTask 
	DoneTask
)

type TaskStatus int

// TODO: change visibility of this enum to this module
const (
	Pending TaskStatus = iota
	Processing
	Done
)

func (ts TaskStatus) String() string {
	return [...]string{"Pending", "Map", "Reduce", "Done"}[ts]
}

type TaskData struct {
	Id            int
	FileLocations []string
	TaskType	  TaskType
}

var DoneTaskData TaskData = TaskData{TaskType: DoneTask}
var EmptyTaskData TaskData = TaskData{TaskType: EmptyTask}

type TaskInfo struct {
	id            int
	status        TaskStatus
	createTs      int
	fileLocations []string
	mutex         sync.Mutex
	taskType	  TaskType
}

func (ti TaskInfo) CreateTs() int {
	return ti.createTs
}

func (ti TaskInfo) GetData() TaskData {
	return TaskData{
		Id: ti.id,
		FileLocations: ti.fileLocations,
		TaskType: ti.taskType,
	}
}

type TaskCol struct {
	Pending map[int]TaskInfo
	Map     map[int]TaskInfo
	Reduce  map[int]TaskInfo
	Done    map[int]TaskInfo
}

