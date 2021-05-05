package mr

import (
	"sync"
)

type TaskStatus int

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
