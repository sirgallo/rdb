package snapshot

import "context"
import "sync/atomic"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (snpService *SnapshotService[T, U, V, W]) Snapshot() error {
	snpService.CurrentSystem.WAL.Mutex.Lock()
	defer snpService.CurrentSystem.WAL.Mutex.Unlock()

	snpService.StateMachine.Mutex.Lock()
	defer snpService.StateMachine.Mutex.Unlock()

	latestLog, latestLogErr := snpService.CurrentSystem.WAL.GetLatest()
	if latestLogErr != nil { return latestLogErr }

	retOpts := &SnapshotHandlerOpts{
		LastIncludedIndex: latestLog.Index,
		LastIncludedTerm: latestLog.Term,
	}

	snaprpc, snapshotErr := snpService.SnapshotHandler(snpService.StateMachine, *retOpts)
	if snapshotErr != nil { return snapshotErr }
	
	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snaprpc)
	if setErr != nil { return setErr }

	_, rpcErr := snpService.ClientSnapshotRPC(snaprpc)
	if rpcErr != nil { return rpcErr }
	
	delErr := snpService.CurrentSystem.WAL.DeleteLogs(latestLog.Index)
	if delErr != nil { return delErr }

	snpService.ResetSystems() 

	return nil
}

func (snpService *SnapshotService[T, U, V, W]) ClientSnapshotRPC(snapshot *snapshotrpc.Snapshot) (bool, error) {
	aliveSystems, minResps := snpService.GetAliveSystemsAndMinSuccessResps()
	successfulResps := int64(0)

	for _, sys := range aliveSystems {
		conn, connErr := snpService.ConnectionPool.GetConnection(sys.Host, snpService.Port)
		if connErr != nil {
			snpService.Log.Error("Failed to connect to", sys.Host + snpService.Port, ":", connErr.Error())
			return false, connErr
		}

		client := snapshotrpc.NewSnapshotServiceClient(conn)

		snapshotRPC := func() (*snapshotrpc.SnapshotResponse, error) {
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			defer cancel()
	
			res, err := client.SnapshotRPC(ctx, snapshot)
			if err != nil { return utils.GetZero[*snapshotrpc.SnapshotResponse](), err }
			return res, nil
		}

		maxRetries := 5
		expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
		expBackoff := utils.NewExponentialBackoffStrat[*snapshotrpc.SnapshotResponse](expOpts)

		res, err := expBackoff.PerformBackoff(snapshotRPC)
		if err != nil {
			snpService.Log.Warn("system", sys.Host, "unreachable, setting status to dead")

			sys.SetStatus(system.Dead)
			snpService.ConnectionPool.CloseConnections(sys.Host)
		}

		if res.Success { atomic.AddInt64(&successfulResps, 1) }
	}

	if successfulResps >= int64(minResps) {
		return true, nil
	} else { return false, nil }
}

func (snpService *SnapshotService[T, U, V, W]) SnapshotRPC(ctx context.Context, req *snapshotrpc.Snapshot) (*snapshotrpc.SnapshotResponse, error) {
	snpService.CurrentSystem.WAL.Mutex.Lock()
	defer snpService.CurrentSystem.WAL.Mutex.Unlock()

	s, ok := snpService.Systems.Load(req.LeaderId)
	if ! ok { 
		sys := &system.System[T]{
			Host: req.LeaderId,
			Status: system.Ready,
		}

		snpService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System[T])
		sys.SetStatus(system.Ready)
	}
	
	setErr := snpService.CurrentSystem.WAL.SetSnapshot(req)
	if setErr != nil { 
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, setErr
	}

	latestLog, latestLogErr := snpService.CurrentSystem.WAL.GetLatest()
	if latestLogErr != nil { 
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, setErr
	}

	delErr := snpService.CurrentSystem.WAL.DeleteLogs(latestLog.Index)
	if delErr != nil {
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, delErr
	}

	return &snapshotrpc.SnapshotResponse{
		Success: true,
	}, nil
}

func (snpService *SnapshotService[T, U, V, W]) ResetSystems() {
	snpService.CurrentSystem.CommitIndex = -1
	snpService.CurrentSystem.LastApplied = -1
	snpService.CurrentSystem.CurrentTerm++

	snpService.Systems.Range(func(key, val interface{}) bool {
		sys := val.(*system.System[T])
		sys.UpdateNextIndex(0)

		return true
	})
}