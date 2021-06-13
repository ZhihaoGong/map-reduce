package mr

import (
	"errors"
	"strconv"
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
	Workers map[int]WorkerInfo
}

func (wc WorkerCol) RegisterWorker(workerId int) {
	// TODO: assert workerId not in the collection
	wc.Workers[workerId] = WorkerInfo{
		id:             workerId,
		status:         IdleWorker,
		lastHeartBeart: time.Now().Unix(),
	}
}

func (wc WorkerCol) UnregisterWorker(workerId int) {
	// TODO: assert workerId in the collection
	if _, ok := wc.Workers[workerId]; ok {
		delete(wc.Workers, workerId)
		return
	}
	panic("WorkerId " + strconv.Itoa(workerId) + " not registered.")
}

func (wc WorkerCol) HasWorker(workerId int) bool {
	_, err := wc.GetWorker(workerId)
	if err == nil {
		return true
	}
	return false
}

func (wc WorkerCol) GetWorker(workerId int) (WorkerInfo, error) {
	if worker, ok := wc.Workers[workerId]; ok {
		return worker, nil
	}
	return WorkerInfo{}, errors.New("Specified workerId not found")
}

func (wc WorkerCol) RenewHeartBeat(workerId int) {
	worker, err := wc.GetWorker(workerId)
	if err != nil {
		panic("Failed to renew workerId " + strconv.Itoa(workerId) + " that not exists.")
	}
	worker.lastHeartBeart = time.Now().Unix()
}

func (wc WorkerCol) CleanDisconnectedWorker(timeoutThedshold int64) {
	now := time.Now().Unix()
	for wid, info := range wc.Workers {
		if info.lastHeartBeart < now-timeoutThedshold {
			wc.UnregisterWorker(wid)
		}
	}
}
