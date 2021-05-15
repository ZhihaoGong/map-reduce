package mr

import (
	"errors"
	"fmt"
)

type Scheduler interface {
	Schedule(worker *WorkerInfo, tasks map[int]*TaskInfo) (int, error)
}

// type FifoScheduler struct{}

// func (rs FifoScheduler) Schedule(worker *WorkerInfo, tasks map[int]*TaskInfo) (int, error) {
// 	first_worker_id := -1
// 	first_worker_create_ts := -1
// 	if len(tasks) == 0 {
// 		return first_worker_id, errors.New("No task available")
// 	}
// 	for id, info := range tasks {
// 		if first_worker_id == -1 || info.createTs() > first_worker_create_ts {
// 			first_worker_id = id
// 			first_worker_create_ts = info.createTs()
// 		}
// 	}
// 	return tasks[first_worker_id], nil
// }

type SimpleScheduler struct{}

func (rs SimpleScheduler) Schedule(worker *WorkerInfo, tasks map[int]*TaskInfo) (int, error) {
	fmt.Println("all tasks", len(tasks))
	for id, info := range tasks {
		info.mutex.Lock()
		fmt.Println("task status", id, info.status)
		if info.status == Pending {
			info.status = Processing
			info.mutex.Unlock()
			return id, nil
		}
		info.mutex.Unlock()
	}
	return 0, errors.New("No task available")
}
