package replog

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Commit


/*
	shared apply log utility function
		1.) transform the logs to pass to the state machine
		2.) pass the transformed entries into the bulk apply function of the state machine, which will perform 
			the state machine operations while applying the logs, returning back the responses to the clients' 
			commands
		3.) block until completed and failed entries are returned
		4.) for all responses:
			if the commit failed: throw an error since the the state machine was incorrectly committed to
			if the commit completed: update the last applied field on the system to the index of the log
				entry
		5.) if the size of the replicated log has exceeded the threshold determined dynamically by available space
			in the current mount and the current node is the leader, trigger a snapshot event to take a snapshot of
			the current state to store and broadcast to all followers, also pause the replicated log and let buffer 
			until the snapshot is complete. If a snapshot is performed, calculate the current system stats to update the
			dynamic threshold for snapshotting
*/

func (rlService *ReplicatedLogService) ApplyLogs() error {
	start := rlService.CurrentSystem.LastApplied + 1 // next to apply after last known applied
	end := rlService.CurrentSystem.CommitIndex  // include up to committed

	var logsToBeApplied []*log.LogEntry
	
	if start == end {
		entry, readErr := rlService.CurrentSystem.WAL.Read(start)
		if entry == nil { return nil }
		if readErr != nil { return readErr }

		logsToBeApplied = append(logsToBeApplied, entry)

	} else {
		entries, rangeErr := rlService.CurrentSystem.WAL.GetRange(start, end)
		if entries == nil { return nil }
		if rangeErr != nil { return rangeErr }

		logsToBeApplied = append(logsToBeApplied, entries...) 
	}

	lastLogToBeApplied := logsToBeApplied[len(logsToBeApplied) - 1]
	
	transform := func(logEntry *log.LogEntry) *statemachine.StateMachineOperation { 
		command := logEntry.Command 
		return &command
	}

	logApplyEntries := utils.Map[*log.LogEntry, *statemachine.StateMachineOperation](logsToBeApplied, transform)

	bulkApplyResps, bulkInserErr := rlService.CurrentSystem.StateMachine.BulkApply(logApplyEntries)
	if bulkInserErr != nil { return bulkInserErr }

	if rlService.CurrentSystem.State == system.Leader {
		for _, resp := range bulkApplyResps {
			rlService.StateMachineResponseChannel <- resp
		}
	} 
	
	rlService.CurrentSystem.UpdateLastApplied(lastLogToBeApplied.Index)

	bucketSizeInBytes, getSizeErr := rlService.CurrentSystem.WAL.GetBucketSizeInBytes()
	if getSizeErr != nil { 
		rlService.Log.Error("error fetching bucket size:", getSizeErr.Error())
		return getSizeErr
	}

	rlService.TriggerSnapshotMutex.Lock()

	triggerSnapshot := func() bool { 
		statsArr, getStatsErr := rlService.CurrentSystem.WAL.GetStats()
		if statsArr == nil || getStatsErr != nil { return false }

		latestObj := statsArr[len(statsArr) - 1]
		thresholdInBytes := latestObj.AvailableDiskSpaceInBytes / FractionOfAvailableSizeToTake // let's keep this small for now

		lastAppliedAtThreshold := bucketSizeInBytes >= thresholdInBytes
		systemAbleToSnapshot := rlService.CurrentSystem.State == system.Leader && rlService.CurrentSystem.Status != system.Busy
		return lastAppliedAtThreshold && systemAbleToSnapshot
	}()

	if triggerSnapshot { 
		rlService.CurrentSystem.SetStatus(system.Busy)
		rlService.SignalStartSnapshot <- true 
	}

	rlService.TriggerSnapshotMutex.Unlock()

	return nil
}