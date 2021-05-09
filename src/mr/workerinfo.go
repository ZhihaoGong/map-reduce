package mr

import (
	"errors"
	"time"
)

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
	status         WorkerStatus
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
		status:         IdleWorker,
		lastHeartBeart: time.Now().Unix(),
	}
}

func (wc WorkerCol) HasWorker(workerId int) bool {
	worker, err := wc.GetWorker(workerId)
	if err == nil {
		return true
	}
	return false
}

func (wc WorkerCol) GetWorker(workerId int) (WorkerInfo, error) {
	if worker, ok := wc.IdleWorker[workerId]; ok {
		return worker, nil
	}
	if worker, ok := wc.MapWorker[workerId]; ok {
		return worker, nil
	}
	if worker, ok := wc.ReduceWorker[workerId]; ok {
		return worker, nil
	}
	return WorkerInfo{}, errors.New("Specified workerId not found")
}
