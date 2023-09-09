package replog

import "errors"

import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Commit


/*
	shared apply log utility function
		1.) transform the logs to pass to the state machine
		2.) pass the transformed entries into the log commit channel where logic will define interacting
			with the state machine
		3.) block until completed and failed entries are returned
		4.) for all responses:
			if the commit failed: throw an error since the the state machine was incorrectly committed to
			if the commit completed: update the last applied field on the system to the index of the log
				entry
*/

func (rlService *ReplicatedLogService[T]) ApplyLogs() error {
	start := rlService.CurrentSystem.LastApplied + 1 // next to apply after last known applied
	end := rlService.CurrentSystem.CommitIndex + 1   // include up to committed

	var logsToBeCommited []*system.LogEntry[T]
	if start == end {
		logsToBeCommited = append(logsToBeCommited, rlService.CurrentSystem.Replog[start])
	} else { logsToBeCommited = append(logsToBeCommited, rlService.CurrentSystem.Replog[start:end]...) }

	transform := func(logEntry *system.LogEntry[T]) LogCommitChannelEntry[T] {
		return LogCommitChannelEntry[T]{
			LogEntry: logEntry,
			Complete: false,
		}
	}

	logCommitEntries := utils.Map[*system.LogEntry[T], LogCommitChannelEntry[T]](logsToBeCommited, transform)

	rlService.LogApplyChan <- logCommitEntries
	completedLogs := <- rlService.LogApplyChan

	for _, completeLog := range completedLogs {
		if ! completeLog.Complete {
			rlService.Log.Debug("incomplete log", completeLog)
			return errors.New("incomplete commit of logs")
		} else { rlService.CurrentSystem.LastApplied = int64(completeLog.LogEntry.Index) }
	}

	return nil
}