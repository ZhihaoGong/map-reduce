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
	createTs   int
	mutex      sync.Mutex
}

func (ti TaskInfo) CreateTs() int {
	return ti.createTs
}

type TaskCol struct {
	Pending map[int]TaskInfo
	Map     map[int]TaskInfo
	Reduce  map[int]TaskInfo
	Done    map[int]TaskInfo
}
