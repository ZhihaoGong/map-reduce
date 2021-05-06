package mr

type Scheduler interface {
	Schedule(worker WorkerInfo, tasks map[int]TaskInfo) (int, error)
}

type FifoScheduler struct{}

func (rs FifoScheduler) Schedule(worker WorkerInfo, tasks map[int]TaskInfo) (int, error) {
	return 0, nil
}
