package mr

import (
	"sync"
)

type TaskStatus int

// TODO: change visibility of this enum to this module
const (
	Pending TaskStatus = iota
	Map
	Reduce
	Done
)

func (ts TaskStatus) String() string {
	return [...]string{"Pending", "Map", "Reduce", "Done"}[ts]
}

type TaskInfo struct {
	id         int
	status     string
	taskStatus string
	mutex      sync.Mutex
}

type TaskCol struct {
	Pending map[int]WorkerInfo
	Map     map[int]WorkerInfo
	Reduce  map[int]WorkerInfo
	Done    map[int]WorkerInfo
}
