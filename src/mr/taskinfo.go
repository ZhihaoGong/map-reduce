package mr

import (
	"sync"
)

type TaskStatus int

const (
	Pending TaskStatus = iota
	Running
	Done
)

func (ts TaskStatus) String() string {
	return [...]string{"Pending", "Running", "Done"}[ts]
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

func (tt TaskType) String() string {
	return [...]string{"Map", "Reduce"}[tt]
}

type TaskInfo struct {
	Id       int
	Status   TaskStatus
	TaskType TaskType
	FileLocations []string
	Mutex    sync.Mutex
}

