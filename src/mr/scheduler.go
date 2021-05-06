package mr

type Scheduler interface {
	Schedule(workers map[int]WorkerInfo, tasks map[int]TaskInfo) (int, error)
}

type FifoScheduler struct{}

func (rs FifoScheduler) Schedule(workers map[int]WorkerInfo, tasks map[int]TaskInfo) (int, error) {
	return 0, nil
}
