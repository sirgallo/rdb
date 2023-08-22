package replog

import "errors"

import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (rlService *ReplicatedLogService[T]) CommitLogsLeader() error {
	logsToBeCommited := rlService.CurrentSystem.Replog[rlService.CurrentSystem.CommitIndex:]
	commitErr := rlService.commitLogs(logsToBeCommited)
	rlService.CurrentSystem.CommitIndex = rlService.CurrentSystem.LastApplied

	if commitErr != nil { return commitErr }

	return nil
}

func (rlService *ReplicatedLogService[T]) CommitLogsFollower() error {
	logsToBeCommited := rlService.CurrentSystem.Replog[rlService.CurrentSystem.LastApplied:rlService.CurrentSystem.CommitIndex]
	commitErr := rlService.commitLogs(logsToBeCommited)
	if commitErr != nil { return commitErr }

	return nil
}

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