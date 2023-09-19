package log

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Log Utils


/*
	Transform Log Entry To Bytes:
		convert entries to byte array to be applied to WAL
*/

func TransformLogEntryToBytes(replog *LogEntry) ([]byte, error) {
	logAsBytes, encErr := utils.EncodeStructToBytes[*LogEntry](replog)
	if encErr != nil { return nil, encErr }

	return logAsBytes, nil
}

/*
	Transform Bytes To Log Entry:
		convert entries from WAL from byte array to log entry
*/

func TransformBytesToLogEntry(data []byte) (*LogEntry, error) {
	logEntry, encErr := utils.DecodeBytesToStruct[LogEntry](data)
	if encErr != nil { return nil, encErr }

	return logEntry, nil
}