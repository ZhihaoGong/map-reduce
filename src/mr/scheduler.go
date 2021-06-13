package mr

import "errors"

type Scheduler interface {
	Schedule(worker WorkerInfo, tasks map[int]TaskInfo) (int, error)
}

type RandomScheduler struct{}

func (rs RandomScheduler) Schedule(worker WorkerInfo, tasks map[int]TaskInfo) (int, error) {
	task_id := -1
	if len(tasks) == 0 {
		return task_id, errors.New("No task available")
	}

	for id, _ := range tasks {
		// Return first found task id
		task_id = id
		break
	}
	return task_id, nil
}
