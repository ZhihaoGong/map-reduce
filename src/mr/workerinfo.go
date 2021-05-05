package mr

import "time"

type WorkerStatus int

// TODO: change visibility of this enum to this module
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
	status         string
	lastHeartBeart int64
}

type WorkerCol struct {
	IdleWorker   map[int]WorkerInfo
	MapWorker    map[int]WorkerInfo
	ReduceWorker map[int]WorkerInfo
}

func (wc WorkerCol) RegisterWorker(workerId int) {
	wc.IdleWorker[workerId] = WorkerInfo{
		id:             workerId,
		status:         IdleWorker.String(),
		lastHeartBeart: time.Now().Unix(),
	}
}

func (wc WorkerCol) HasWorker(workerId int) bool {
	if _, ok := wc.IdleWorker[workerId]; ok {
		return true
	}
	if _, ok := wc.MapWorker[workerId]; ok {
		return true
	}
	if _, ok := wc.ReduceWorker[workerId]; ok {
		return true
	}
	return false
}
