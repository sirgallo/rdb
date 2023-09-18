package stats

import "os"
import "syscall"
import "time"


import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"


var Log = clog.NewCustomLog(NAME)


func CalculateCurrentStats [T log.MachineCommands]() (*Stats, error) {
	path, dirErr := os.Getwd()
	if dirErr != nil { return nil, dirErr }
	
	/*
	dirInfo, infoErr := os.Stat(path)
	if infoErr != nil { return nil, infoErr }
	*/

	var stat syscall.Statfs_t

	statErr := syscall.Statfs(path, &stat) 
	if statErr != nil {
		Log.Error("error getting disk space for", path, ":", statErr.Error())
		return nil, statErr
	}

	blockSize := uint64(stat.Bsize)
	available := int64(stat.Bavail * blockSize)
	total := int64(stat.Blocks * blockSize)
	used := int64((stat.Blocks - stat.Bfree) * blockSize)

	currTime := time.Now()
	formattedTime := currTime.Format(time.RFC3339)
	
	return &Stats{
		AvailableDiskSpaceInBytes: available,
		TotalDiskSpaceInBytes: total,
		UsedDiskSpaceInBytes: used,
		Timestamp: formattedTime,
	}, nil
}