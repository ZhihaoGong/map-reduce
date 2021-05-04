package mr

type WorkerStatus int

const (
	IdleWorker WorkerStatus = iota
	MapWorker
	ReduceWorker
)

func (ws WorkerStatus) String() string {
	return [...]string{"Idle", "Map", "Reduce"}[ws]
}

type WorkerInfo struct {
	id             int
	status         WorkerStatus
	lastHeartBeart int64
}
