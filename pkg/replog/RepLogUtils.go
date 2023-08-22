package replog

import "github.com/sirgallo/raft/pkg/system"


func (rlService *ReplicatedLogService[T]) checkIndex(index int64) bool {
	if index >= 0 && index < int64(len(rlService.CurrentSystem.Replog)) { return true }
	return false
}

func (rlService *ReplicatedLogService[T]) previousLogTerm(nextIndex int64) int64 {
	repLog := rlService.CurrentSystem.Replog[rlService.CurrentSystem.NextIndex]
	return repLog.Term
}

func (rlService *ReplicatedLogService[T]) DeferenceLogEntries() []system.LogEntry[T] {
	var logEntries []system.LogEntry[T]
	for _, log := range rlService.CurrentSystem.Replog {
		logEntries = append(logEntries, *log)
	}

	return logEntries
}