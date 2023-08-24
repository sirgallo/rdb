package replog

import "log"

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
		logBatches, _ := utils.Chunk[*replogrpc.LogEntry](req.AppendEntry.Entries, rlService.determineBatchSize())
		earliestBatch := logBatches[0]
		
		prevLogIndex := req.AppendEntry.PrevLogIndex
		prevLogTerm := req.AppendEntry.PrevLogTerm

		transformLog := func(entries []*replogrpc.LogEntry) ReplicatedLogRequest {
			appendEntry := replogrpc.AppendEntry{
				Term: req.AppendEntry.Term,
				LeaderId: req.AppendEntry.LeaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: earliestBatch,
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
	get the previous term and index for a system in the cluster:
		NextIndex == -1 
			--> this is a new system, or one that was declared Dead or lost it's replicated log (sys failure, etc.)
			--> in this situation, get the earliest log index and term available in the replog

		NextIndex > -1
			--> this is an existing system
			--> get the NextIndex field on the system object (which is tracked through responses from AppendEntryRPCs)
*/

func (rlService *ReplicatedLogService[T]) determinePreviousIndexAndTerm(sys *system.System[T]) (int64, int64) {
	var prevLogIndex, prevLogTerm int64
	
	previousLogTerm := func(nextIndex int64) int64 {
		repLog := rlService.CurrentSystem.Replog[rlService.CurrentSystem.NextIndex]
		return repLog.Term
	}

	if sys.NextIndex == -1 {
		prevLogIndex = 0
		if len(rlService.CurrentSystem.Replog) > 0 {
			prevLogTerm = rlService.CurrentSystem.Replog[0].Term
		} else {  prevLogTerm = 0 }
	} else {
		prevLogIndex = sys.NextIndex
		prevLogTerm = previousLogTerm(sys.NextIndex)
	}

	return prevLogIndex, prevLogTerm
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

	entriesToSend := rlService.CurrentSystem.Replog[prevLogIndex:]
	entries := utils.Map[*system.LogEntry[T], *replogrpc.LogEntry](entriesToSend, transformLogEntry)

	appendEntry := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	return appendEntry
}