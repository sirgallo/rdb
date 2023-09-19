package replog

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/stats"
import "github.com/sirgallo/raft/pkg/statemachine"
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

func (rlService *ReplicatedLogService) ApplyLogs() error {
	start := rlService.CurrentSystem.LastApplied + 1 // next to apply after last known applied
	end := rlService.CurrentSystem.CommitIndex  // include up to committed

	var logsToBeApplied []*log.LogEntry
	
	if start == end {
		entry, readErr := rlService.CurrentSystem.WAL.Read(start)
		if readErr != nil { return readErr }

		if entry != nil { logsToBeApplied = append(logsToBeApplied, entry) }
	} else {
		entries, rangeErr := rlService.CurrentSystem.WAL.GetRange(start, end)
		if rangeErr != nil { return rangeErr }

		logsToBeApplied = append(logsToBeApplied, entries...) 
	}

	lastLogApplied := logsToBeApplied[len(logsToBeApplied) - 1]
	
	transform := func(logEntry *log.LogEntry) *statemachine.StateMachineOperation { 
		command := logEntry.Command 
		return &command
	}

	logApplyEntries := utils.Map[*log.LogEntry, *statemachine.StateMachineOperation](logsToBeApplied, transform)

	_, bulkInserErr := rlService.CurrentSystem.StateMachine.BulkApply(logApplyEntries)
	if bulkInserErr != nil { return bulkInserErr }
	
	rlService.CurrentSystem.LastApplied = lastLogApplied.Index

	bucketSizeInBytes, getSizeErr := rlService.CurrentSystem.WAL.GetBucketSizeInBytes()
	if getSizeErr != nil { 
		rlService.Log.Error("error fetching bucket size:", getSizeErr.Error())
		return getSizeErr
	}

	triggerSnapshot := func() bool { 
		statsArr, getStatsErr := rlService.CurrentSystem.WAL.GetStats()
		if statsArr == nil || getStatsErr != nil { return false }

		latestObj := statsArr[len(statsArr) - 1]
		thresholdInBytes := latestObj.AvailableDiskSpaceInBytes / 4

		lastAppliedAtThreshold := bucketSizeInBytes >= thresholdInBytes
		return lastAppliedAtThreshold && rlService.CurrentSystem.State == system.Leader
	}()

	if triggerSnapshot {
		go func () { rlService.SignalStartSnapshot <- true }()
		go func () { rlService.PauseReplogSignal <- true }()

		statObj, calcErr := stats.CalculateCurrentStats()
		if calcErr != nil { return calcErr }

		setErr := rlService.CurrentSystem.WAL.SetStat(*statObj)
		if setErr != nil { return setErr }
	}

	return nil
}