package snapshot

import "context"
import "errors"
import "sync/atomic"
import "sync"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (snpService *SnapshotService[T, U, V, W]) Snapshot() error {
	lastAppliedLog, readErr := snpService.CurrentSystem.WAL.Read(snpService.CurrentSystem.LastApplied)
	if readErr != nil { return readErr }

	retOpts := &SnapshotHandlerOpts{
		LastIncludedIndex: lastAppliedLog.Index,
		LastIncludedTerm: lastAppliedLog.Term,
	}

	snaprpc, snapshotErr := snpService.SnapshotHandler(snpService.StateMachine, *retOpts)
	if snapshotErr != nil { return snapshotErr }
	
	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snaprpc)
	if setErr != nil { return setErr }

	ok, rpcErr := snpService.BroadcastSnapshotRPC(snaprpc)
	if rpcErr != nil { return rpcErr }
	if ! ok { return errors.New("snapshot not received and processed by min followers") }

	delErr := snpService.CurrentSystem.WAL.DeleteLogs(lastAppliedLog.Index - 1)
	if delErr != nil { 
		snpService.Log.Error("error deleting logs")
		return delErr 
	}

	return nil
}

func (snpService *SnapshotService[T, U, V, W]) UpdateIndividualSystem(host string) error {
	s, _ := snpService.Systems.Load(host)
	sys := s.(*system.System[T])

	sys.SetStatus(system.Busy)

	snpService.Log.Info("updating snapshot for system:", sys.Host)

	snapshot, getErr := snpService.CurrentSystem.WAL.GetSnapshot()
	if getErr != nil { 
		snpService.Log.Error("error getting snapshot for system")
		return getErr 
	}

	_, rpcErr := snpService.ClientSnapshotRPC(sys, snapshot)
	if rpcErr != nil { 
		snpService.Log.Error("rpc error when sending snapshot:", rpcErr.Error())
		return rpcErr
	}

	sys.UpdateNextIndex(snapshot.LastIncludedIndex)
	sys.SetStatus(system.Ready)

	return nil
}

func (snpService *SnapshotService[T, U, V, W]) BroadcastSnapshotRPC(snapshot *snapshotrpc.Snapshot) (bool, error) {
	aliveSystems, minResps := snpService.GetAliveSystemsAndMinSuccessResps()
	successfulResps := int64(0)

	var snapshotWG sync.WaitGroup

	for _, sys := range aliveSystems {
		snapshotWG.Add(1)
		go func (sys *system.System[T]) {
			defer snapshotWG.Done()
	
			res, rpcErr := snpService.ClientSnapshotRPC(sys, snapshot)
			if rpcErr != nil { 
				snpService.Log.Error("rpc error when sending snapshot:", rpcErr.Error())
				return
			}

			snpService.Log.Debug("res for sys", sys.Host, ":", res)
			if res.Success { atomic.AddInt64(&successfulResps, 1) }
		}(sys)
	}

	snapshotWG.Wait()

	if successfulResps >= int64(minResps) {
		snpService.Log.Info("minimum successful responses received on snapshot broadcast", successfulResps)
		return true, nil
	} else { 
		snpService.Log.Warn("minimum successful responses not received on snapshot broadcast", successfulResps)
		return false, nil 
	}
}

func (snpService *SnapshotService[T, U, V, W]) ClientSnapshotRPC(sys *system.System[T], snapshot *snapshotrpc.Snapshot) (*snapshotrpc.SnapshotResponse, error) {
	conn, connErr := snpService.ConnectionPool.GetConnection(sys.Host, snpService.Port)
	if connErr != nil {
		snpService.Log.Error("Failed to connect to", sys.Host + snpService.Port, ":", connErr.Error())
		return nil, connErr
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

		return nil, err
	}

	return res, nil
}

func (snpService *SnapshotService[T, U, V, W]) SnapshotRPC(ctx context.Context, req *snapshotrpc.Snapshot) (*snapshotrpc.SnapshotResponse, error) {
	setErr := snpService.CurrentSystem.WAL.SetSnapshot(req)
	if setErr != nil { 
		snpService.Log.Error("set err:", setErr.Error())
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, setErr
	}

	lastIncluded, readErr := snpService.CurrentSystem.WAL.Read(req.LastIncludedIndex)
	if readErr != nil { 
		snpService.Log.Error("read err:", readErr.Error())
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, setErr
	}

	delErr := snpService.CurrentSystem.WAL.DeleteLogs(lastIncluded.Index - 1)
	if delErr != nil {
		snpService.Log.Error("delete err:", delErr.Error())
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, delErr
	}

	snpService.Log.Info("snapshot from leader processed, returning successful response")
	
	return &snapshotrpc.SnapshotResponse{
		Success: true,
	}, nil
}