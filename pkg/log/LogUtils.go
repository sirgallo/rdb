package log

import "github.com/sirgallo/raft/pkg/utils"


/*
	Transform Log Entry To Bytes:
		convert entries from in mem log to byte array to be applied to WAL
*/

func TransformLogEntryToBytes [T MachineCommands](replog *LogEntry[T]) ([]byte, error) {
	logAsBytes, encErr := utils.EncodeStructToBytes[*LogEntry[T]](replog)
	if encErr != nil { return nil, encErr }

	return logAsBytes, nil
}

/*
	Transform Bytes To Log Entry:
		convert entries from WAL from byte array to log entry
*/

func TransformBytesToLogEntry [T MachineCommands](data []byte) (*LogEntry[T], error) {
	// dataWithoutNewline := strings.TrimSuffix(string(data), "\n")
	logEntry, encErr := utils.DecodeBytesToStruct[LogEntry[T]](data)
	if encErr != nil { return nil, encErr }

	return logEntry, nil
}