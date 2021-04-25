package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskApplyReq struct {
	WorkerID int
}

type TaskApplyRes struct {
	TaskId   int
	TaskType string
}

type StatusReportMsg struct {
	WorkerID int
}

type StatusReportAck struct {
	Received bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
