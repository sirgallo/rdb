package snapshot

import "os"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/wal"
import "github.com/sirgallo/raft/pkg/system"


//=========================================== Snapshot Utils


/*
	Process Incoming Snapshot RPC
		run in a separate go routine
			1.) open a new file with the filepath included in the snapshot entry and write the compressed stream in the snapshot
				to the file
			2.) set the snapshot entry in the snapshot index in the replog bolt db for lookups
			3.) if both complete, get the last included log in the replicated log and delete all logs up to it
*/

func (snpService *SnapshotService) ProcessIncomingSnapshotRPC(req *snapshotrpc.SnapshotChunk) error {
	writeErr := os.WriteFile(req.SnapshotFilePath, req.SnapshotChunk, os.ModePerm)
  if writeErr != nil { return writeErr }

	snapshotEntry := &wal.SnapshotEntry{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm: req.LastIncludedTerm,
		SnapshotFilePath: req.SnapshotFilePath,
	}

	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	if setErr != nil { return setErr }

	lastIncluded, readErr := snpService.CurrentSystem.WAL.Read(req.LastIncludedIndex)
	if readErr != nil { return setErr }

	delErr := snpService.CurrentSystem.WAL.DeleteLogs(lastIncluded.Index - 1)
	if delErr != nil { return delErr }

	return nil
}

/*
	Get Alive Systems And Min Success Resps:
		helper method for both determining the current alive systems in the cluster and also the minimum successful responses
		needed for committing logs to the state machine

		--> minimum is found by floor(total alive systems / 2) + 1
*/

func (snpService *SnapshotService) GetAliveSystemsAndMinSuccessResps() ([]*system.System, int) {
	var aliveSystems []*system.System

	snpService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System)
		if sys.Status == system.Ready { aliveSystems = append(aliveSystems, sys) }

		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}