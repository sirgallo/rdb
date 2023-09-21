package service

import "github.com/sirgallo/raft/pkg/stats"


//=========================================== Raft Utils


/*
	Update RepLog On Startup:
		on system startup or restart replay the WAL
			1.) get the latest log from the WAL on disk
			2.) update commit index to last log index from synced WAL --> WAL only contains committed logs
			3.) update current term to term of last log
*/

func (raft *RaftService) UpdateRepLogOnStartup() (bool, error) {
	snapshotEntry, snapshotErr := raft.CurrentSystem.WAL.GetSnapshot()
	if snapshotErr != nil { return false, snapshotErr }

	if snapshotEntry != nil { 
		replayErr := raft.CurrentSystem.StateMachine.ReplaySnapshot(snapshotEntry.SnapshotFilePath) 
		if replayErr != nil { return false, replayErr }
		Log.Info("latest snapshot found and replayed successfully")
	}

	lastLog, latestErr := raft.CurrentSystem.WAL.GetLatest()

	if latestErr != nil {
		return false, latestErr
	} else if lastLog != nil {
		raft.CurrentSystem.CommitIndex = lastLog.Index

		applyErr := raft.ReplicatedLog.ApplyLogs()
		if applyErr != nil { return false, applyErr }

		total, totalErr := raft.CurrentSystem.WAL.GetTotal()
		if totalErr != nil { return false , totalErr }

		Log.Info("total entries on startup:", total)
	}

	return true, nil
}

func (raft *RaftService) InitStats() error {
	initStatObj, calcErr := stats.CalculateCurrentStats()
	if calcErr != nil {
		Log.Error("unable to get calculate stats for path", calcErr.Error())
		return calcErr
	}

	statSetErr := raft.CurrentSystem.WAL.SetStat(*initStatObj)
	if statSetErr != nil {
		Log.Error("unable to get set stats in bucket", statSetErr.Error())
		return statSetErr
	}

	return nil
}