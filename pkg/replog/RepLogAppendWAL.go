package replog 

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/statemachine"


func (rlService *ReplicatedLogService) AppendWALSync(cmd *statemachine.StateMachineOperation) error {
	lastLogIndex, _, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { return lastLogErr }

	nextIndex := lastLogIndex + 1

	newLog := &log.LogEntry{
		Index: nextIndex,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: *cmd,
	}

	appendErr := rlService.CurrentSystem.WAL.Append(newLog)
	if appendErr != nil {
		rlService.Log.Error("append error:", appendErr.Error())
		return appendErr 
	}

	select {
		case rlService.AppendedChannel <- true:
		default:
	}

	return nil
}