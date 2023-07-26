package replog 

import "context"
import "log"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	min := func (idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}
	
	sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.LeaderId })[0]
	system.SetStatus[T](sys, true)

	var resp *replogrpc.AppendEntryResponse

	if req.Term < rlService.CurrentSystem.CurrentTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			Success: false,
		}
	}

	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	if req.PrevLogIndex >= lastLogIndex || rlService.CurrentSystem.Replog[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			LatestLogIndex: lastLogIndex,
			Success: false,
		}
	}

	rlService.LeaderAcknowledgedSignal <- true

	for _, entry := range req.Entries {
		if rlService.checkIndex(entry.Index) {
			if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term { 
				rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[entry.Index:] 
			}
		}

		cmd, decErr := utils.DecodeStringToStruct[T](entry.Command)
		if decErr != nil { log.Println("error on decode -->", decErr) }
		newLog := &system.LogEntry[T]{
			Index: entry.Index,
			Term: entry.Term,
			Command: *cmd,
		}

		rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
	}

	// log.Println("replog -->", rlService.deferenceLogEntries())

	if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
		index := int64(len(rlService.CurrentSystem.Replog) - 1)
		rlService.CurrentSystem.CommitIndex = min(req.LeaderCommitIndex, index)
	}

	lastLogIndexAfterAppend, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	resp = &replogrpc.AppendEntryResponse{
		Term: rlService.CurrentSystem.CurrentTerm,
		LatestLogIndex: lastLogIndexAfterAppend,
		Success: true,
	}

	return resp, nil
}