package replog 

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/statemachine"


//=========================================== RepLog Append WAL


/*
	As new requests are received, append the logs in order to the replicated log/WAL
*/

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

	return nil
}