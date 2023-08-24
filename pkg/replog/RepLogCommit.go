package replog

import "errors"

import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Commit


// NOTE: Incomplete, TODO

/*
	Commit Logs Leader 
*/

func (rlService *ReplicatedLogService[T]) CommitLogsLeader() error {
	logsToBeCommited := rlService.CurrentSystem.Replog[rlService.CurrentSystem.CommitIndex:]
	commitErr := rlService.commitLogs(logsToBeCommited)
	rlService.CurrentSystem.CommitIndex = rlService.CurrentSystem.LastApplied

	if commitErr != nil { return commitErr }

	return nil
}

/*
	Commit Logs for Follower systems 
*/

func (rlService *ReplicatedLogService[T]) CommitLogsFollower() error {
	start := rlService.CurrentSystem.LastApplied
	end := rlService.CurrentSystem.CommitIndex

	logsToBeCommited := rlService.CurrentSystem.Replog[start:end]
	commitErr := rlService.commitLogs(logsToBeCommited)
	if commitErr != nil { return commitErr }

	return nil
}

/*
	shared commit log utility function
		1.) transform the logs to pass to the state machine
		2.) pass the transformed entries into the log commit channel where logic will define interacting 
			with the state machine
		3.) block until completed and failed entries are returned 
		4.) for all responses:
			if the commit failed: throw an error since the the state machine was incorrectly committed to
			if the commit completed: update the last applied field on the system to the index of the log 
				entry
*/

func (rlService *ReplicatedLogService[T]) commitLogs(logsToBeCommited []*system.LogEntry[T]) error {
	transform := func (logEntry *system.LogEntry[T]) LogCommitChannelEntry[T] {
		return LogCommitChannelEntry[T]{
			LogEntry: logEntry,
			Complete: false,
		}
	}

	logCommitEntries := utils.Map[*system.LogEntry[T], LogCommitChannelEntry[T]](logsToBeCommited, transform)

	rlService.LogCommitChannel <- logCommitEntries
	completedLogs :=<- rlService.LogCommitChannel
	
	for _, completeLog := range completedLogs {
		if ! completeLog.Complete {
			return errors.New("incomplete commit of logs")
		} else { rlService.CurrentSystem.LastApplied = int64(completeLog.LogEntry.Index) }
	}

	return nil
}