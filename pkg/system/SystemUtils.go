package system

import "strings"

import "github.com/sirgallo/raft/pkg/utils"

//=========================================== System Utils

/*
	Determine Last Log Idx And Term:
		get the last index and term from the replicated log
		1.) if the log length is greater than 0
			get the log at the end of the replicated log and return its index and term
		2.) otherwise
			we can assume this is a new system, so we default the index to -1 and term to 0
			to indicate this
*/

func DetermineLastLogIdxAndTerm [T MachineCommands](replog []*LogEntry[T]) (int64, int64) {
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
	Replay Logs On Start:
		on start or restart, read from the WAL
			1.) transform from logs from byte array to log entry
			2.) return logs
*/
func (system *System[T]) ReplayLogsOnStart() ([]*LogEntry[T], error) {
	logsAsBytes, replayErr := system.WAL.Replay()
	if replayErr != nil { return nil, replayErr }

	if len(logsAsBytes) > 0 {
		transform := func(data []byte) *LogEntry[T] {
			entry, _ := TransformBytesToLogEntry[T](data)
			return entry
		}

		return utils.Map[[]byte, *LogEntry[T]](logsAsBytes, transform), nil
	} else { return nil, nil }
}

/*
	Transform Log Entry To Bytes:
		convert entries from in mem log to byte array to be applied to WAL
*/

func TransformLogEntryToBytes [T MachineCommands](replog *LogEntry[T]) ([]byte, error) {
	logAsBytes, encErr := utils.EncodeStructToBytes[*LogEntry[T]](replog)
	if encErr != nil { return nil, encErr }

	return append(logAsBytes, '\n'), nil
}

/*
	Transform Bytes To Log Entry:
		convert entries from WAL from byte array to log entry
*/

func TransformBytesToLogEntry [T MachineCommands](data []byte) (*LogEntry[T], error) {
	dataWithoutNewline := strings.TrimSuffix(string(data), "\n")
	logEntry, encErr := utils.DecodeBytesToStruct[LogEntry[T]]([]byte(dataWithoutNewline))
	if encErr != nil { return nil, encErr }

	return logEntry, nil
}