package replog

import "context"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Follower

/*
	AppendEntryRPC:
		grpc server implementation

		when an AppendEntryRPC is made to the appendEntry server
			1.) if the host of the incoming request is not in the systems map, store it
			2.) reset the election timeout regardless of success or failure response
			3.) if the request has a term lower than the current term of the system
				--> return a failure response with the term of the system
			4.) if the term of the replicated log on the system is not the term of the request or is not present
				--> return a failure response, with the term and the index of the request - 1 to update NextIndex
			5.) acknowledge that the request is legitimate and send signal to reset the leader election timeout
			6.) for all of the entries of the incoming request
				--> if the term of the replicated log associated with the index of the incoming entry is not the same
					as the request, remove up to the entry in the log on the system and begin appending logs
				--> otherwise, just append the incoming entries to in memory log and WAL, since we can consider the log
					committed on the follower
			7.) if the commit index of the incoming request is higher than on the system, commit logs up to the commit index from
					last applied for the state machine on the system
			8.) if logs are at least up to date with the leader's commit index:
				--> return a success response with the index of the latest log applied to the replicated log
					else:
				--> return a failed response so the follower can sync itself up to the leader if inconsistent log length
*/

func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	s, ok := rlService.Systems.Load(req.LeaderId)
	if ! ok {
		sys := &system.System[T]{
			Host: req.LeaderId,
			Status: system.Ready,
		}

		rlService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System[T])
		sys.SetStatus(system.Ready)
	}

	rlService.attemptLeadAckSignal()

	failedNextIndex := func() int64 {
		replogLength := len(rlService.CurrentSystem.Replog)
		if replogLength == 0 || req.PrevLogIndex - 1 < 0 { return 0 }

		return req.PrevLogIndex - 1
	}()

	handleReqTerm := func() bool { return req.Term >= rlService.CurrentSystem.CurrentTerm }
	handleReqValidTermAtIndex := func() bool {
		if len(rlService.CurrentSystem.Replog) == 0 || req.Entries == nil { return true } // special case for when a system has empty replicated log or hearbeats where we don't check
		return rlService.CheckIndex(req.PrevLogIndex) && rlService.CurrentSystem.Replog[req.PrevLogIndex].Term == req.PrevLogTerm
	}

	reqTermOk := handleReqTerm()
	if ! reqTermOk {
		rlService.Log.Warn("request term lower than current term, returning failed response")
		return rlService.generateResponse(failedNextIndex, false), nil
	}

	reqTermValid := handleReqValidTermAtIndex()
	if ! reqTermValid {
		rlService.Log.Warn("log at request previous index has mismatched term or does not exist, returning failed response")
		return rlService.generateResponse(failedNextIndex, false), nil
	}

	rlService.CurrentSystem.SetCurrentLeader(req.LeaderId)
	_, repLogErr := rlService.HandleReplicateLogs(req)
	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	nextLogIndex := lastLogIndex + 1
	
	if repLogErr != nil {
		rlService.Log.Warn("replog err:", repLogErr.Error())
		return rlService.generateResponse(nextLogIndex, false), repLogErr
	}

	if lastLogIndex < req.LeaderCommitIndex && req.Entries != nil {
		rlService.Log.Warn("log length inconsistent with leader log length")
		return rlService.generateResponse(nextLogIndex, false), nil
	}

	successfulResp := rlService.generateResponse(nextLogIndex, true)
	rlService.Log.Info("leader acknowledged and returning successful response:", successfulResp)

	return successfulResp, nil
}

/*
	Handle Replicate Logs:
		helper method used for both replicating the logs to the follower's replicated log and also for committing logs to
		the state machine up to the leader's last commit index
*/

func (rlService *ReplicatedLogService[T]) HandleReplicateLogs(req *replogrpc.AppendEntry) (bool, error) {
	min := func(idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}

	if req.Entries != nil {
		appendLogToReplicatedLog := func(entry *replogrpc.LogEntry) error {
			cmd, decErr := utils.DecodeStringToStruct[T](entry.Command)
			if decErr != nil {
				rlService.Log.Error("error on decode -->", decErr.Error())
				return decErr
			}

			newLog := &system.LogEntry[T]{
				Index: entry.Index,
				Term: entry.Term,
				Command: *cmd,
			}

			rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)

			logAsBytes, encErr := system.TransformLogEntryToBytes[T](newLog)
			if encErr != nil { rlService.Log.Error("encode err:", encErr.Error()) }

			rlService.CurrentSystem.WAL.WriteStream <- logAsBytes

			return nil
		}

		for _, entry := range req.Entries {
			if rlService.CheckIndex(entry.Index) {
				if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term {
					rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[:entry.Index+1]
					decErr := appendLogToReplicatedLog(entry)
					if decErr != nil { return false, decErr }
				}
			} else {
				decErr := appendLogToReplicatedLog(entry)
				if decErr != nil { return false, decErr }
			}
		}
	}

	if rlService.CheckIndex(req.LeaderCommitIndex) {
		if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
			lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
			minCommitIndex := min(req.LeaderCommitIndex, lastLogIndex) + 1

			rlService.CurrentSystem.CommitIndex = minCommitIndex
			commitErr := rlService.CommitLogsFollower()
			if commitErr != nil { return false, commitErr }
		}
	}

	return true, nil
}

func (rlService *ReplicatedLogService[T]) generateResponse(lastLogIndex int64, success bool) *replogrpc.AppendEntryResponse {
	return &replogrpc.AppendEntryResponse{
		Term: rlService.CurrentSystem.CurrentTerm,
		NextLogIndex: lastLogIndex,
		Success: success,
	}
}