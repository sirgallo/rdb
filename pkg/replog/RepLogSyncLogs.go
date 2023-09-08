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
*/

func (rlService *ReplicatedLogService[T]) SyncLogs(host string) (bool, error) {
	s, _ := rlService.Systems.Load(host)
	sys := s.(*system.System[T])

	sys.SetStatus(system.Busy)

	conn, connErr := rlService.ConnectionPool.GetConnection(sys.Host, rlService.Port)
	if connErr != nil {
		rlService.Log.Error("Failed to connect to", sys.Host+rlService.Port, ":", connErr.Error())
		return false, connErr
	}

	for {
		req := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: rlService.PrepareAppendEntryRPC(sys.NextIndex, false),
		}

		res, err := rlService.clientAppendEntryRPC(conn, sys, req)
		if err != nil { return false, err }
		if res.Success {
			sys.SetStatus(system.Ready)
			rlService.ConnectionPool.PutConnection(sys.Host, conn)

			return true, nil
		}
	}
}