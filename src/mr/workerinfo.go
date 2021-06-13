package mr

import (
	"errors"
	"fmt"
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
	workers               map[int]WorkerInfo
	disconnestedThedshold int64
	nextWorkerId          int
}

func (wc WorkerCol) RegisterWorker(workerId int) {
	// TODO: assert workerId not in the collection
	wc.workers[workerId] = WorkerInfo{
		id:             workerId,
		status:         IdleWorker,
		lastHeartBeart: time.Now().Unix(),
	}
}

func (wc WorkerCol) UnregisterWorker(workerId int) {
	// TODO: assert workerId in the collection
	if _, ok := wc.workers[workerId]; ok {
		delete(wc.workers, workerId)
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
	if worker, ok := wc.workers[workerId]; ok {
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

func (wc WorkerCol) CleanDisconnectedWorker() {
	now := time.Now().Unix()

	fmt.Printf("cleaning disconnected worker\n")

	for wid, info := range wc.workers {
		if info.lastHeartBeart < now-wc.disconnestedThedshold {
			wc.UnregisterWorker(wid)
		}
	}
}

func (wc WorkerCol) GetNextWorkerId() int {
	wc.nextWorkerId++
	return wc.nextWorkerId
}
