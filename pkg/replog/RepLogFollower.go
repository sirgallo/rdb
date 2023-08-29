package replog

import "context"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Follower


/*
	AppendEntryRPC:
		grpc implementation

		when an AppendEntryRPC is made to the appendEntry server
			1.) if the host of the incoming request is not in the systems map, store it
			2.) reset the election timeout regardless of success or failure response
			2.) if the request has a term lower than the current term of the system
				--> return a failure response with the term of the system
			3.) if the term of the replicated log on the system is not the term of the request or is not present
				--> return a failure response, with the term and the index of the request - 1 to update NextIndex
			4.) acknowledge that the request is legitimate and send signal to reset the leader election timeout
			5.) for all of the entries of the incoming request
				--> if the term of the replicated log associated with the index of the incoming entry is not the same
					as the request, remove up to the entry in the log on the system and begin appending logs
				--> otherwise, just append the incoming entries
			6.) if the commit index of the incoming request is higher than on the system, commit up the commit index
				for the state machine on the system
			7.) return a success response with the index of the latest log applied to the replicated log
*/

func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	sys := &system.System[T]{
		Host: req.LeaderId,
		NextIndex: -1,
	}

	rlService.Systems.LoadOrStore(sys.Host, sys)

	rlService.LeaderAcknowledgedSignal <- true
	success := true
	
	handleReqTerm := func() bool { 
		return req.Term >= rlService.CurrentSystem.CurrentTerm
	}
	handleReqValidTermAtReqIndex := func() bool {
		if len(rlService.CurrentSystem.Replog) == 0 || req.Entries == nil { return true }	// special case for when a system has empty replicated log or hearbeats where we don't check
		return rlService.checkIndex(req.PrevLogIndex) && rlService.CurrentSystem.Replog[req.PrevLogIndex].Term == req.PrevLogTerm
	}

	success = handleReqTerm()
	if ! success { 
		rlService.Log.Debug("request term lower than current term, returning failed response")
		return rlService.generateResponse(req.PrevLogIndex - 1, success), nil 
	}

	success = handleReqValidTermAtReqIndex()
	if ! success { 
		rlService.Log.Debug("log at request previous index has mismatched term or does not exist, returning failed response")
		return rlService.generateResponse(req.PrevLogIndex - 1, success), nil 
	}

	success, repLogErr := rlService.HandleReplicateLogs(req)
	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	if repLogErr != nil { 
		rlService.Log.Warn("replog err:", repLogErr)
		return rlService.generateResponse(lastLogIndex, success), repLogErr 
	}

	rlService.Log.Debug("leader acknowledged and returning successful response:", rlService.generateResponse(lastLogIndex, success))
	return rlService.generateResponse(lastLogIndex, success), nil
}

func (rlService *ReplicatedLogService[T]) HandleReplicateLogs(req *replogrpc.AppendEntry) (bool, error) {
	if req.Entries != nil {
		min := func(idx1, idx2 int64) int64 {
			if idx1 < idx2 { return idx1 }
			return idx2
		}

		appendLogToReplicatedLog := func(entry *replogrpc.LogEntry) error {
			cmd, decErr := utils.DecodeStringToStruct[T](entry.Command)
			if decErr != nil {
				rlService.Log.Error("error on decode -->", decErr)
				return decErr
			}

			newLog := &system.LogEntry[T]{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: *cmd,
			}

			rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
			return nil
		}

		for _, entry := range req.Entries {
			if rlService.checkIndex(entry.Index) {
				if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term {
					rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[:entry.Index]
					decErr := appendLogToReplicatedLog(entry)
					if decErr != nil { return false, decErr }
				}
			} else {
				decErr := appendLogToReplicatedLog(entry)
				if decErr != nil { return false, decErr }
			}
		}

		if rlService.checkIndex(req.LeaderCommitIndex) {
			if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
				index := int64(len(rlService.CurrentSystem.Replog) - 1)
				rlService.CurrentSystem.CommitIndex = min(req.LeaderCommitIndex, index)
				commitErr := rlService.CommitLogsFollower()
				if commitErr != nil { return false, commitErr }
			}
		}
	}

	return true, nil
}

func (rlService *ReplicatedLogService[T]) generateResponse(lastLogIndex int64, success bool) *replogrpc.AppendEntryResponse {
	return &replogrpc.AppendEntryResponse{
		Term:           rlService.CurrentSystem.CurrentTerm,
		LatestLogIndex: lastLogIndex,
		Success:        success,
	}
}