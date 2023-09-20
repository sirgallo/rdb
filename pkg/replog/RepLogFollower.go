package replog

import "context"
import "errors"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
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
				--> return a failure response, with the earliest known index for the term, or from the latest term known on the 
					follower to update NextIndex
			5.) acknowledge that the request is legitimate and send signal to reset the leader election timeout
			6.) for all of the entries of the incoming request
				--> if the term of the replicated log associated with the index of the incoming entry is not the same
					as the request, remove up to the entry in the log on the system and begin appending logs
				--> otherwise, just prepare the batch of logs to be range appended to the WAL
			7.) if the commit index of the incoming request is higher than on the system, commit logs up to the commit index from
					last applied for the state machine on the system
			8.) if logs are at least up to date with the leader's commit index:
				--> return a success response with the index of the latest log applied to the replicated log
					else:
				--> return a failed response so the follower can sync itself up to the leader if inconsistent log length
*/

func (rlService *ReplicatedLogService) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	s, ok := rlService.Systems.Load(req.LeaderId)
	if ! ok {
		sys := &system.System{
			Host: req.LeaderId,
			Status: system.Ready,
		}

		rlService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		sys.SetStatus(system.Ready)
	}

	rlService.attemptLeadAckSignal()

	total, totalErr := rlService.CurrentSystem.WAL.GetTotal()
	if totalErr != nil { return nil, totalErr }

	latestKnownLogTerm := int64(0)

	lastLog, getLastLogErr := rlService.CurrentSystem.WAL.GetLatest()
	if getLastLogErr != nil { return nil, getLastLogErr }
	if lastLog != nil { latestKnownLogTerm = lastLog.Term }
	
	failedIndexToFetch := req.PrevLogIndex - 1

	lastIndexedLog, lastIndexedErr := rlService.CurrentSystem.WAL.GetIndexedEntryForTerm(latestKnownLogTerm)
	if lastIndexedErr != nil { return nil, lastIndexedErr }
	if lastIndexedLog != nil { failedIndexToFetch = lastIndexedLog.Index }

	failedNextIndex, failedIndexErr := func() (int64, error) {
		if total == 0 || failedIndexToFetch < 0 { return 0, nil }
		return failedIndexToFetch, nil
	}()

	if failedIndexErr != nil { return nil, failedIndexErr }

	handleReqTerm := func() bool { return req.Term >= rlService.CurrentSystem.CurrentTerm }
	handleReqValidTermAtIndex := func() (bool, error) {
		currEntry, readErr := rlService.CurrentSystem.WAL.Read(req.PrevLogIndex)
		if readErr != nil { return false, readErr }

		if total == 0 || req.Entries == nil { return true, nil } // special case for when a system has empty replicated log or hearbeats where we don't check
		return currEntry != nil && currEntry.Term == req.PrevLogTerm, nil
	}

	reqTermOk := handleReqTerm()
	if ! reqTermOk {
		rlService.Log.Warn("request term lower than current term, returning failed response")
		return rlService.generateResponse(failedNextIndex, false), nil
	}

	if (rlService.CurrentSystem.CurrentLeader != req.LeaderId) { rlService.CurrentSystem.SetCurrentLeader(req.LeaderId) }

	reqTermValid, readErr := handleReqValidTermAtIndex()
	if readErr != nil { return rlService.generateResponse(failedNextIndex, false), readErr }
	
	if ! reqTermValid {
		rlService.Log.Warn("log at request previous index has mismatched term or does not exist, returning failed response")
		return rlService.generateResponse(failedNextIndex, false), nil
	}

	_, repLogErr := rlService.HandleReplicateLogs(req)
	if repLogErr != nil { 
		rlService.Log.Error("rep log handle error:", repLogErr.Error())
		return rlService.generateResponse(failedNextIndex, false), repLogErr
	}

	lastLogIndex, _, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { 
		rlService.Log.Error("error getting last log index", lastLogErr)
		return rlService.generateResponse(failedNextIndex, false), lastLogErr 
	}

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
	rlService.Log.Info("leader", req.LeaderId, "acknowledged and returning successful response:", successfulResp)

	return successfulResp, nil
}

/*
	Handle Replicate Logs:
		helper method used for both replicating the logs to the follower's replicated log and also for applying logs to
		the state machine up to the leader's last commit index or last known log on the system if it is less than the 
		commit index of the leader

		instead of appending one at a time, we can batch all of the log entries into a single bolt db transaction to reduce 
		overhead and total transactions performed on the db, which should improve performance
*/

func (rlService *ReplicatedLogService) HandleReplicateLogs(req *replogrpc.AppendEntry) (bool, error) {
	min := func(idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}

	logTransform := func(entry *replogrpc.LogEntry) *log.LogEntry {
		cmd, decErr := utils.DecodeStringToStruct[statemachine.StateMachineOperation](entry.Command)
		if decErr != nil {
			rlService.Log.Error("error on decode -->", decErr.Error())
			return nil
		}

		return &log.LogEntry{
			Index: entry.Index,
			Term: entry.Term,
			Command: *cmd,
		}
	}

	var logsToAppend []*log.LogEntry

	appendLogToReplicatedLog := func(entry *replogrpc.LogEntry) error {
		newLog := logTransform(entry)
		if newLog == nil { return errors.New("log transform failed, new log is null") }
		
		logsToAppend = append(logsToAppend, newLog)

		return nil
	}

	if req.Entries != nil {
		for idx, entry := range req.Entries {
			currEntry, readErr := rlService.CurrentSystem.WAL.Read(entry.Index)
			if readErr != nil { return false, readErr }

			if currEntry != nil {
				if currEntry.Term != entry.Term {
					transformedLogs := utils.Map[*replogrpc.LogEntry, *log.LogEntry](req.Entries[:idx + 1], logTransform)
					rangeUpdateErr := rlService.CurrentSystem.WAL.RangeAppend(transformedLogs)
					if rangeUpdateErr != nil { return false, rangeUpdateErr }
				}
			} else {
				appendErr := appendLogToReplicatedLog(entry)
				if appendErr != nil { return false, appendErr }
			}
		}

		rlService.CurrentSystem.WAL.RangeAppend(logsToAppend)
	}

	logAtCommitIndex, readErr := rlService.CurrentSystem.WAL.Read(req.LeaderCommitIndex)
	if readErr != nil { return false, readErr }

	if logAtCommitIndex != nil {
		if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
			lastLogIndex, _, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
			if lastLogErr != nil { return false, lastLogErr }
		
			minCommitIndex := min(req.LeaderCommitIndex, lastLogIndex)
			rlService.CurrentSystem.CommitIndex = minCommitIndex
			
			applyErr := rlService.ApplyLogs()
			if applyErr != nil { return false, applyErr }
		}
	}

	return true, nil
}

func (rlService *ReplicatedLogService) generateResponse(lastLogIndex int64, success bool) *replogrpc.AppendEntryResponse {
	return &replogrpc.AppendEntryResponse{
		Term: rlService.CurrentSystem.CurrentTerm,
		NextLogIndex: lastLogIndex,
		Success: success,
	}
}