package replog

import "log"
import "time"

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

func (rlService *ReplicatedLogService[T]) determineBatchSize() int{
	return 10000
}

/*
	existance check on the log for a specific index, ensure that it can exist within range
*/

func (rlService *ReplicatedLogService[T]) checkIndex(index int64) bool {
	return (index >= 0 && index < int64(len(rlService.CurrentSystem.Replog)))
}

/*
	prepare an AppendEntryRPC:
		--> determine what entries to get, which will be the previous log index forward for that particular system
		--> encode the command entries to string
		--> create the rpc request from the Log Entry
*/

func (rlService *ReplicatedLogService[T]) prepareAppendEntryRPC(nextIndex int64, isHeartbeat bool) *replogrpc.AppendEntry {
	sysHostPtr := &rlService.CurrentSystem.Host

	transformLogEntry := func(logEntry *system.LogEntry[T]) *replogrpc.LogEntry {
		cmd, err := utils.EncodeStructToString[T](logEntry.Command)
		if err != nil { log.Println("error encoding log struct to string") }
		
		return &replogrpc.LogEntry{
			Index: logEntry.Index,
			Term: logEntry.Term,
			Command: cmd,
		}
	}

	var previousLogIndex, previousLogTerm int64

	if nextIndex == 0 {
		previousLogIndex = utils.GetZero[int64]() 
		previousLogTerm = utils.GetZero[int64]()
	} else {
		previousLog := rlService.CurrentSystem.Replog[nextIndex - 1]
		previousLogIndex = previousLog.Index
		previousLogTerm = previousLog.Term
	}

	var entries []*replogrpc.LogEntry

	if isHeartbeat {
		entries = nil
	} else {
		entriesToSend := func() []*system.LogEntry[T] {
			batchSize := rlService.determineBatchSize()

			if nextIndex == 0 { 
				if len(rlService.CurrentSystem.Replog) <= batchSize {
					return rlService.CurrentSystem.Replog 
				} else { return rlService.CurrentSystem.Replog[:batchSize] }
			}

			if len(rlService.CurrentSystem.Replog[nextIndex:]) <= batchSize { 
				return rlService.CurrentSystem.Replog[nextIndex:]
			} else { return rlService.CurrentSystem.Replog[nextIndex:batchSize] }
		}()

		entries = utils.Map[*system.LogEntry[T], *replogrpc.LogEntry](entriesToSend, transformLogEntry)
	}

	appendEntry := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm: previousLogTerm,
		Entries: entries,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	return appendEntry
}

func (rlService *ReplicatedLogService[T]) GetAliveSystemsAndMinSuccessResps() ([]*system.System[T], int) {
	var aliveSystems []*system.System[T]
	
	rlService.Systems.Range(func(key, value interface{}) bool {
		aliveSystems = append(aliveSystems, value.(*system.System[T]))
		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}

func (rlService *ReplicatedLogService[T]) resetTimer() {
	if ! rlService.HeartBeatTimer.Stop() {
    select {
			case <- rlService.HeartBeatTimer.C:
			default:
		}
	}

	rlService.HeartBeatTimer.Reset(HeartbeatIntervalInMs * time.Millisecond)
}