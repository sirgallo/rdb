package replog

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Utils


/*
	Determine Batch Size:
		TODO: not implemented

		just use 10000 for testing right now --> TODO, make this dynamic
		maybe find a way to get latest network MB/s and avg log size and determine based on this
*/

func (rlService *ReplicatedLogService) determineBatchSize() int {
	return 10000
}

/*
	prepare an AppendEntryRPC:
		--> determine what entries to get, which will be the next log index forward for that particular system
		--> batch the entries
		--> encode the command entries to string
		--> create the rpc request from the Log Entry
*/

func (rlService *ReplicatedLogService) PrepareAppendEntryRPC(lastLogIndex int64, nextIndex int64, isHeartbeat bool) (*replogrpc.AppendEntry, error) {
	transformLogEntry := func(logEntry *log.LogEntry) *replogrpc.LogEntry {
		cmd, encErr := utils.EncodeStructToString[statemachine.StateMachineOperation](logEntry.Command)
		if encErr != nil { 
			rlService.Log.Error("error encoding log struct to string") 
			return nil
		}

		return &replogrpc.LogEntry{
			Index: logEntry.Index,
			Term: logEntry.Term,
			Command: cmd,
		}
	}

	var previousLogIndex, previousLogTerm int64
	var entries []*replogrpc.LogEntry

	lastLog, lastLogErr := rlService.CurrentSystem.WAL.Read(lastLogIndex)
	if lastLogErr != nil { return nil, lastLogErr }

	if isHeartbeat {
		if lastLogIndex >= 0 {
			previousLogIndex = lastLogIndex
		} else { previousLogIndex = utils.GetZero[int64]() }

		if lastLog == nil {
			lastLog = &log.LogEntry{
				Term: utils.GetZero[int64](),
			}
		}
		
		previousLogTerm = lastLog.Term
		entries = nil
	} else {
		previousLog, readErr := rlService.CurrentSystem.WAL.Read(nextIndex - 1)
		if readErr != nil { return nil, readErr }

		if previousLog != nil {
			previousLogIndex = previousLog.Index
			previousLogTerm = previousLog.Term
		} else {
			previousLogIndex = utils.GetZero[int64]()
			previousLogTerm = utils.GetZero[int64]()
		}

		entriesToSend, entriesErr := func() ([]*log.LogEntry, error) {
			batchSize := rlService.determineBatchSize()
			totalToSend := lastLogIndex - nextIndex

			if totalToSend <= int64(batchSize) {
				allEntries, rangeErr := rlService.CurrentSystem.WAL.GetRange(nextIndex, lastLogIndex)
				if rangeErr != nil { return nil, rangeErr }

				return allEntries, nil
			} else { 
				indexUpToBatch := nextIndex + int64(batchSize - 1)
				entriesInBatch, rangeErr := rlService.CurrentSystem.WAL.GetRange(nextIndex, indexUpToBatch)
				if rangeErr != nil { return nil, rangeErr }

				return entriesInBatch, nil
			}
		}()

		if entriesErr != nil { return nil, entriesErr }

		entries = utils.Map[*log.LogEntry, *replogrpc.LogEntry](entriesToSend, transformLogEntry)
	}

	appendEntry := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: rlService.CurrentSystem.Host,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm: previousLogTerm,
		Entries: entries,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	return appendEntry, nil
}

/*
	Get Alive Systems And Min Success Resps:
		helper method for both determining the current alive systems in the cluster and also the minimum successful responses
		needed for committing logs to the state machine

		--> minimum is found by floor(total alive systems / 2) + 1
*/

func (rlService *ReplicatedLogService) GetAliveSystemsAndMinSuccessResps() ([]*system.System, int) {
	var aliveSystems []*system.System

	rlService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System)
		if sys.Status == system.Ready { aliveSystems = append(aliveSystems, sys) }

		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}

/*
	Reset Heartbeat Timer:
		used to reset the heartbeat timer:
			--> if unable to stop the timer, drain the timer
			--> reset the timer with the heartbeat interval
*/

func (rlService *ReplicatedLogService) resetHeartbeatTimer() {
	if ! rlService.HeartBeatTimer.Stop() {
		select {
			case <-rlService.HeartBeatTimer.C:
			default:
		}
	}

	rlService.HeartBeatTimer.Reset(HeartbeatInterval)
}

/*
	Reset Replog Timer:
		same as above
*/

func (rlService *ReplicatedLogService) resetReplogTimer() {
	if ! rlService.ReplicateLogsTimer.Stop() {
		select {
			case <-rlService.ReplicateLogsTimer.C:
			default:
		}
	}

	rlService.ReplicateLogsTimer.Reset(RepLogInterval)
}