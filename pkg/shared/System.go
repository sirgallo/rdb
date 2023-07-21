package shared


func DetermineLastLogIdxAndTerm [T comparable](replog []*LogEntry[T]) (int64, int64) {
	logLength := len(replog)
	var lastLogIndex, lastLogTerm int64
	
	if logLength > 0 {
		lastLog := replog[logLength - 1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = 0
		lastLogTerm = 0
	}

	return lastLogIndex, lastLogTerm
}