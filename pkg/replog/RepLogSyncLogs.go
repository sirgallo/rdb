package replog

import "github.com/sirgallo/raft/pkg/system"


//=========================================== Sync Logs


/*
	Sync Logs:
		helper method for handling syncing followers who have inconsistent logs

		while unsuccessful response:
			send AppendEntryRPC to follower with logs starting at the follower's NextIndex
			if error: return false, error
			on success: return true, nil --> the log is now up to date with the leader
		
		if the earliest log on the leader is greater then the next index of the system, 
		send a signal to send the latest snapshot to the follower and perform log replication,
		since this means that follower was not in the cluster during the latest log compaction event
*/

func (rlService *ReplicatedLogService) SyncLogs(host string) (bool, error) {
	s, _ := rlService.Systems.Load(host)
	sys := s.(*system.System)

	sys.SetStatus(system.Busy)

	conn, connErr := rlService.ConnectionPool.GetConnection(sys.Host, rlService.Port)
	if connErr != nil {
		rlService.Log.Error("Failed to connect to", sys.Host + rlService.Port, ":", connErr.Error())
		return false, connErr
	}

	for {
		earliestLog, earliestErr := rlService.CurrentSystem.WAL.GetEarliest()
		if earliestErr != nil { return false, earliestErr }


		lastLogIndex, _, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
		if lastLogErr != nil { return false, lastLogErr }

		preparedEntries, prepareErr := rlService.PrepareAppendEntryRPC(lastLogIndex, sys.NextIndex, false)
		if prepareErr != nil { return false, prepareErr }

		req := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: preparedEntries,
		}

		res, rpcErr := rlService.clientAppendEntryRPC(conn, sys, req)
		if rpcErr != nil { return false, rpcErr }

		if res.Success {
			sys.SetStatus(system.Ready)
			rlService.ConnectionPool.PutConnection(sys.Host, conn)

			return true, nil
		} else if earliestLog != nil && sys.NextIndex < earliestLog.Index {
			rlService.SendSnapshotToSystemSignal <- sys.Host
			return true, nil
		}
	}
}