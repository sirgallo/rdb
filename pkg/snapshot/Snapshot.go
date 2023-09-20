package snapshot

import "context"
import "errors"
import "os"
import "sync/atomic"
import "sync"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "github.com/sirgallo/raft/pkg/wal"


//=========================================== RepLog Service


/*
	Snapshot the current state of of the state machine and the replicated log
		1.) get the latest known log entry, to set on the snapshotrpc for last included index and term
			of logs in the snapshot
		2.) snapshot the current state machine 
			--> since the db is a boltdb instance, we'll take a backup of the data and gzipped, returning 
			the filepath where the snapshot is stored on the system as a reference
		3.) set the snapshot entry in the replicated log db in an index
			--> the entry contains last included index, last included term, and the filepath on the system
				to the latest snapshot --> snapshots are stored durably and snapshot entry can be seen as a pointer
		4.) broadcast the new snapshot to all other systems. so send the entry with the filepath and file compressed
				to byte representation
		5.) if the broadcast was successful, delete all logs in the replicated log up to the last included log
			--> so we compact the log only when we know the broadcast received the minimum number of successful 
				responses in the quorum
*/

func (snpService *SnapshotService) Snapshot() error {
	lastAppliedLog, readErr := snpService.CurrentSystem.WAL.Read(snpService.CurrentSystem.LastApplied)
	if readErr != nil { return readErr }

	snapshotFile, snapshotErr := snpService.CurrentSystem.StateMachine.SnapshotStateMachine()
	if snapshotErr != nil { return snapshotErr }
	
	snapshotContent, readErr := os.ReadFile(snapshotFile)
	if readErr != nil { return readErr }

	snaprpc := &snapshotrpc.Snapshot{
		LastIncludedIndex: lastAppliedLog.Index,
		LastIncludedTerm: lastAppliedLog.Term,
		SnapshotFilePath: snapshotFile,
		Snapshot: snapshotContent,
	}

	snapshotEntry := &wal.SnapshotEntry{
		LastIncludedIndex: lastAppliedLog.Index,
		LastIncludedTerm: lastAppliedLog.Term,
		SnapshotFilePath: snapshotFile,
	}

	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
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

/*
	Update Individual System
		when a node restarts and rejoins the cluster or a new node is added
			1.) set the node to send to busy so interactions with other followers continue
			2.) create a snapshotrpc for the node that contains the last snapshot
			2.) send the rpc to the node
			3. if successful, update the next index for the node to the last included index
				and set the system status to ready
*/

func (snpService *SnapshotService) UpdateIndividualSystem(host string) error {
	s, _ := snpService.Systems.Load(host)
	sys := s.(*system.System)

	sys.SetStatus(system.Busy)

	snpService.Log.Info("updating snapshot for system:", sys.Host)

	snapshot, getErr := snpService.CurrentSystem.WAL.GetSnapshot()
	if getErr != nil { 
		snpService.Log.Error("error getting snapshot for system")
		return getErr 
	}

	snapshotContent, readErr := os.ReadFile(snapshot.SnapshotFilePath)
	if readErr != nil { return readErr }

	snaprpc := &snapshotrpc.Snapshot{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm: snapshot.LastIncludedTerm,
		SnapshotFilePath: snapshot.SnapshotFilePath,
		Snapshot: snapshotContent,
	}

	_, rpcErr := snpService.ClientSnapshotRPC(sys, snaprpc)
	if rpcErr != nil { 
		snpService.Log.Error("rpc error when sending snapshot:", rpcErr.Error())
		return rpcErr
	}

	sys.UpdateNextIndex(snapshot.LastIncludedIndex)
	sys.SetStatus(system.Ready)

	return nil
}

/*
	Shared Broadcast RPC function:
		for requests to be broadcasted:
			1.) send SnapshotRPCs in parallel to each follower in the cluster
			2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
			3.)
				if err: remove system from system map and close all connections -- it has failed
				if res:
					if success:
						--> update total successful replies
			4.) if total successful responses are greater than minimum, return success, otherwise failed
*/

func (snpService *SnapshotService) BroadcastSnapshotRPC(snapshot *snapshotrpc.Snapshot) (bool, error) {
	aliveSystems, minResps := snpService.GetAliveSystemsAndMinSuccessResps()
	successfulResps := int64(0)

	var snapshotWG sync.WaitGroup

	for _, sys := range aliveSystems {
		snapshotWG.Add(1)
		go func (sys *system.System) {
			defer snapshotWG.Done()
	
			res, rpcErr := snpService.ClientSnapshotRPC(sys, snapshot)
			if rpcErr != nil { 
				snpService.Log.Error("rpc error when sending snapshot:", rpcErr.Error())
				return
			}

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

/*
	Client Append Entry RPC:
		helper method for making individual rpc calls

		perform exponential backoff
		--> success: return successful response
		--> error: remove system from system map and close all open connections
*/

func (snpService *SnapshotService) ClientSnapshotRPC(sys *system.System, snapshot *snapshotrpc.Snapshot) (*snapshotrpc.SnapshotResponse, error) {
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

/*
	SnapshotRPC
		grpc server implementation

		when snapshot is sent to the snapshotrpc server
			1.) open a new file with the filepath included in the snapshot entry and write the compressed stream in the snapshot
				to the file
			2.) set the snapshot entry in the snapshot index in the replog bolt db for lookups
			3.) if both complete, get the last included log in the replicated log and delete all logs up to it
			4.) return response to leader
*/

func (snpService *SnapshotService) SnapshotRPC(ctx context.Context, req *snapshotrpc.Snapshot) (*snapshotrpc.SnapshotResponse, error) {
	writeErr := os.WriteFile(req.SnapshotFilePath, req.Snapshot, os.ModePerm)
  if writeErr != nil { 
		return &snapshotrpc.SnapshotResponse{
			Success: false,
		}, writeErr 
	}

	snapshotEntry := &wal.SnapshotEntry{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm: req.LastIncludedTerm,
		SnapshotFilePath: req.SnapshotFilePath,
	}

	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	
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