package log

import "github.com/sirgallo/raft/pkg/statemachine"


type LogEntry struct {
	Index int64
	Term int64
	Command statemachine.StateMachineOperation
}