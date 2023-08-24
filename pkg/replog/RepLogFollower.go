package replog 

import "context"
import "log"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Follower


/*
	AppendEntryRPC:
		grpc implementation

		when an AppendEntryRPC is made to the appendEntry server
			1.) if the host of the incoming request has been Dead, set the status as Alive
			2.) if the request has a term lower than the current term of the system
				--> return a failure response with the term of the system
			3.) if the term of the replicated log on the system is not the term of the request
				--> return a failure response
			4.) acknowledge that the request is legitamate and send signal to reset the leader election timeout
			5.) for all of the entries of the incoming request
				--> if the term of the replicated log associated with the index of the incoming entry is not the same
					as the request, remove up to the entry in the log on the system and begin appending logs
				--> otherwise, just append the incoming entries
			6.) if the commit index of the incoming request is higher than on the system, commit up the commit index
				for the state machine on the system
			7.) return a success response with the index of the latest log applied to the replicated log
*/

func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	min := func (idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}
	
	sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.LeaderId })[0]
	system.SetStatus[T](sys, true)

	var resp *replogrpc.AppendEntryResponse

	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	if req.Term < rlService.CurrentSystem.CurrentTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			LatestLogIndex: lastLogIndex,
			Success: false,
		}
	}

	if rlService.checkIndex(req.PrevLogIndex) && rlService.CurrentSystem.Replog[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			LatestLogIndex: lastLogIndex,
			Success: false,
		}
	}

	rlService.LeaderAcknowledgedSignal <- true // acknowledge only if truthy

	appendLogToReplicatedLog := func(entry *replogrpc.LogEntry) {
		cmd, decErr := utils.DecodeStringToStruct[T](entry.Command)
		if decErr != nil { log.Println("error on decode -->", decErr) }
		
		newLog := &system.LogEntry[T]{
			Index: entry.Index,
			Term: entry.Term,
			Command: *cmd,
		}
	
		rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
	}

	if req.Entries != nil {
		for _, entry := range req.Entries {
			if rlService.checkIndex(entry.Index) {
				if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term { 
					rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[:entry.Index] 
					appendLogToReplicatedLog(entry)
				}
			} else { appendLogToReplicatedLog(entry) }
		}
	
		if rlService.checkIndex(req.LeaderCommitIndex) {
			if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
				index := int64(len(rlService.CurrentSystem.Replog) - 1)
				rlService.CurrentSystem.CommitIndex = min(req.LeaderCommitIndex, index)
				rlService.CommitLogsFollower()
			}
		}
	}

	lastLogIndexAfterAppend, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	resp = &replogrpc.AppendEntryResponse{
		Term: rlService.CurrentSystem.CurrentTerm,
		LatestLogIndex: lastLogIndexAfterAppend,
		Success: true,
	}

	return resp, nil
}