package replog


import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Utils


/*
	Determine Batch Size:
		TODO: not implemented

		just use 10000 for testing right now --> TODO, make this dynamic
		maybe find a way to get latest network MB/s and avg log size and determine based on this
*/

func (rlService *ReplicatedLogService[T]) determineBatchSize() int {
	return 10000
}

/*
	prepare an AppendEntryRPC:
		--> determine what entries to get, which will be the next log index forward for that particular system
		--> batch the entries
		--> encode the command entries to string
		--> create the rpc request from the Log Entry
*/

func (rlService *ReplicatedLogService[T]) PrepareAppendEntryRPC(nextIndex int64, isHeartbeat bool) (*replogrpc.AppendEntry, error) {
	sysHostPtr := &rlService.CurrentSystem.Host

	transformLogEntry := func(logEntry *log.LogEntry[T]) *replogrpc.LogEntry {
		cmd, err := utils.EncodeStructToString[T](logEntry.Command)
		if err != nil { rlService.Log.Debug("error encoding log struct to string") }

		return &replogrpc.LogEntry{
			Index: logEntry.Index,
			Term: logEntry.Term,
			Command: cmd,
		}
	}

	var previousLogIndex, previousLogTerm int64
	var entries []*replogrpc.LogEntry

	lastLogIndex, lastLogTerm, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { return nil, lastLogErr }

	if isHeartbeat {
		if lastLogIndex >= 0 {
			previousLogIndex = lastLogIndex
		} else { previousLogIndex = utils.GetZero[int64]() }

		previousLogTerm = lastLogTerm
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

		entriesToSend, entriesErr := func() ([]*log.LogEntry[T], error) {
			batchSize := rlService.determineBatchSize()

			total, totalErr := rlService.CurrentSystem.WAL.GetTotal(nextIndex, previousLogIndex)
			if totalErr != nil { return nil, totalErr }

			if total <= batchSize {
				allEntries, rangeErr := rlService.CurrentSystem.WAL.GetRange(nextIndex, lastLogIndex)
				if rangeErr != nil { return nil, rangeErr }

				return allEntries, nil
			} else { 
				indexUpToBatch := nextIndex + int64(batchSize)
				entriesInBatch, rangeErr := rlService.CurrentSystem.WAL.GetRange(nextIndex, indexUpToBatch)
				if rangeErr != nil { return nil, rangeErr }

				return entriesInBatch, nil
			}
		}()

		if entriesErr != nil { return nil, entriesErr }

		entries = utils.Map[*log.LogEntry[T], *replogrpc.LogEntry](entriesToSend, transformLogEntry)
	}

	appendEntry := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
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

func (rlService *ReplicatedLogService[T]) GetAliveSystemsAndMinSuccessResps() ([]*system.System[T], int) {
	var aliveSystems []*system.System[T]

	rlService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System[T])
		if sys.Status == system.Ready { aliveSystems = append(aliveSystems, sys) }

		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}

/*
	Reset Timer:
		used to reset the heartbeat timer:
			--> if unable to stop the timer, drain the timer
			--> reset the timer with the heartbeat interval
*/

func (rlService *ReplicatedLogService[T]) resetTimer() {
	if ! rlService.HeartBeatTimer.Stop() {
		select {
			case <-rlService.HeartBeatTimer.C:
			default:
		}
	}

	rlService.HeartBeatTimer.Reset(HeartbeatInterval)
}