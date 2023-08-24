package replog

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (rlService *ReplicatedLogService[T]) checkIndex(index int64) bool {
	if index >= 0 && index < int64(len(rlService.CurrentSystem.Replog)) { return true }
	return false
}

func (rlService *ReplicatedLogService[T]) previousLogTerm(nextIndex int64) int64 {
	repLog := rlService.CurrentSystem.Replog[rlService.CurrentSystem.NextIndex]
	return repLog.Term
}

func (rlService *ReplicatedLogService[T]) batchRequests(requests []ReplicatedLogRequest)[][]ReplicatedLogRequest {
	// just use 10000 for testing right now
	batchSize := 10000

	var batchedRequests [][]ReplicatedLogRequest
	for _, req := range requests {
		entriesChunks, _ := utils.Chunk[*replogrpc.LogEntry](req.AppendEntry.Entries, batchSize)
		
		prevLogIndex := req.AppendEntry.PrevLogIndex
		prevLogTerm := req.AppendEntry.PrevLogTerm

		transform := func(entries []*replogrpc.LogEntry) ReplicatedLogRequest {
			appendEntry := replogrpc.AppendEntry{
				Term: req.AppendEntry.Term,
				LeaderId: req.AppendEntry.LeaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: entries,
			}

			if len(entries) > 0 {
				lastLog := entries[len(entries) - 1]
				prevLogIndex = lastLog.Index
				prevLogTerm = lastLog.Term
			}

			return ReplicatedLogRequest{
				Host: req.Host,
				AppendEntry: &appendEntry,
			}
		}

		requestChunks := utils.MapChunks[*replogrpc.LogEntry, ReplicatedLogRequest](entriesChunks, transform)
		batchedRequests = append(batchedRequests, requestChunks)
	}

	return batchedRequests
}

func (rlService *ReplicatedLogService[T]) DeferenceLogEntries() []system.LogEntry[T] {
	var logEntries []system.LogEntry[T]
	for _, log := range rlService.CurrentSystem.Replog {
		logEntries = append(logEntries, *log)
	}

	return logEntries
}