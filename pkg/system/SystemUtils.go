package system


//=========================================== System Utils


/*
	get the last index and term from the replicated log
	1.) if the log length is greater than 0
		get the log at the end of the replicated log and return its index and term
	2.) otherwise
		we can assume this is a new system, so we default the index to -1 and term to 0
		to indicate this
*/

func DetermineLastLogIdxAndTerm [T comparable](replog []*LogEntry[T]) (int64, int64) {
	logLength := len(replog)
	var lastLogIndex, lastLogTerm int64
	
	if logLength > 0 {
		lastLog := replog[logLength - 1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = -1 // -1 symbolizes empty log
		lastLogTerm = 0
	}

	return lastLogIndex, lastLogTerm
}

/*
	the the status of a particular system in the systems list to either Alive or Dead

	TODO: dynamically add new systems to the list
*/

func SetStatus [T comparable](system *System[T], isAlive bool) {
	if isAlive { 
		system.Status = Alive 
	} else { 
		system.Status = Dead 
		system.NextIndex = -1
	}
}