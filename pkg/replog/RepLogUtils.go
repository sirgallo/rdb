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
	to minimize system and network overhead, requests take only a specified number of logs. 
*/

func (rlService *ReplicatedLogService[T]) truncatedRequests(requests []ReplicatedLogRequest) []ReplicatedLogRequest {
	transformLogRequest := func(req ReplicatedLogRequest) ReplicatedLogRequest {
		// logBatches, _ := utils.Chunk[*replogrpc.LogEntry](req.AppendEntry.Entries, rlService.determineBatchSize())
		// earliestBatch := logBatches[0]
		batchSize := rlService.determineBatchSize()
		
		var earliestBatch []*replogrpc.LogEntry
		
		if len(req.AppendEntry.Entries) <= batchSize {
			earliestBatch = req.AppendEntry.Entries
		} else { earliestBatch = req.AppendEntry.Entries[:batchSize - 1] }

		transformLog := func(entries []*replogrpc.LogEntry) ReplicatedLogRequest {
			appendEntry := replogrpc.AppendEntry{
				Term: req.AppendEntry.Term,
				LeaderId: req.AppendEntry.LeaderId,
				PrevLogIndex: req.AppendEntry.PrevLogIndex,
				PrevLogTerm: req.AppendEntry.PrevLogTerm,
				Entries: entries,
			}

			return ReplicatedLogRequest{
				Host: req.Host,
				AppendEntry: &appendEntry,
			}
		}

		return transformLog(earliestBatch)
	}

	return utils.Map[ReplicatedLogRequest, ReplicatedLogRequest](requests, transformLogRequest)
}

/*
	existance check on the log for a specific index, ensure that it can exist within range
*/

func (rlService *ReplicatedLogService[T]) checkIndex(index int64) bool {
	if index >= 0 && index < int64(len(rlService.CurrentSystem.Replog)) { return true }
	return false
}

/*
	prepare an AppendEntryRPC:
		--> determine what entries to get, which will be the previous log index forward for that particular system
		--> encode the command entries to string
		--> create the rpc request from the Log Entry
*/

func (rlService *ReplicatedLogService[T]) prepareAppendEntryRPC(prevLogIndex, prevLogTerm int64) *replogrpc.AppendEntry {
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

	var entries []*replogrpc.LogEntry
	var previousLogIndex, previousLogTerm int64

	if prevLogIndex == -1 {
		entries = nil
		previousLogIndex = utils.GetZero[int64]() 
		previousLogTerm = utils.GetZero[int64]()
	} else {
		entriesToSend := rlService.CurrentSystem.Replog[prevLogIndex:]
		entries = utils.Map[*system.LogEntry[T], *replogrpc.LogEntry](entriesToSend, transformLogEntry)

		previousLogIndex = prevLogIndex
		previousLogTerm = prevLogTerm
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
	aliveSystems := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { 
		return sys.Status == system.Alive 
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