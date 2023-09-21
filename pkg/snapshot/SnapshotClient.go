package snapshot

import "context"
import "errors"
import "io"
import "os"
import "sync/atomic"
import "sync"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
// import "github.com/sirgallo/raft/pkg/utils"
import "github.com/sirgallo/raft/pkg/wal"


//=========================================== Snapshot Client


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
	snpService.CurrentSystem.SetStatus(system.Busy)

	lastAppliedLog, readErr := snpService.CurrentSystem.WAL.Read(snpService.CurrentSystem.LastApplied)
	if readErr != nil { return readErr }

	snapshotFile, snapshotErr := snpService.CurrentSystem.StateMachine.SnapshotStateMachine()
	if snapshotErr != nil { return snapshotErr }
	
	snpService.Log.Info("snapshot created with filepath:", snapshotFile)

	snapshotEntry := &wal.SnapshotEntry{
		LastIncludedIndex: lastAppliedLog.Index,
		LastIncludedTerm: lastAppliedLog.Term,
		SnapshotFilePath: snapshotFile,
	}

	setErr := snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	if setErr != nil { return setErr }

	delErr := snpService.CurrentSystem.WAL.DeleteLogs(lastAppliedLog.Index - 1)
	if delErr != nil { 
		snpService.Log.Error("error deleting logs")
		return delErr 
	}

	initSnapShotRPC := &snapshotrpc.SnapshotChunk{
		LastIncludedIndex: lastAppliedLog.Index,
		LastIncludedTerm: lastAppliedLog.Term,
		SnapshotFilePath: snapshotFile,
	}

	ok, rpcErr := snpService.BroadcastSnapshotRPC(initSnapShotRPC)
	if rpcErr != nil { return rpcErr }
	if ! ok { return errors.New("snapshot not received and processed by min followers") }

	snpService.CurrentSystem.SetStatus(system.Ready)
	
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

	snaprpc := &snapshotrpc.SnapshotChunk{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm: snapshot.LastIncludedTerm,
		SnapshotFilePath: snapshot.SnapshotFilePath,
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

func (snpService *SnapshotService) BroadcastSnapshotRPC(snapshot *snapshotrpc.SnapshotChunk) (bool, error) {
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
	Client Snapshot RPC:
		helper method for making individual rpc calls

		a grpc stream is used here
			--> open the file and read the snapshot file in chunks, passing the chunks into the snapshot stream channel
			--> as new chunks enter the channel, send them to the target follower
			--> close the snapshot stream when the file has been read, finish passing the rest of the chunks, 
				and return the response from closing the stream
*/

func (snpService *SnapshotService) ClientSnapshotRPC(sys *system.System, initSnapshotShotReq *snapshotrpc.SnapshotChunk) (*snapshotrpc.SnapshotStreamResponse, error) {
	conn, connErr := snpService.ConnectionPool.GetConnection(sys.Host, snpService.Port)
	if connErr != nil {
		snpService.Log.Error("Failed to connect to", sys.Host + snpService.Port, ":", connErr.Error())
		return nil, connErr
	}

	client := snapshotrpc.NewSnapshotServiceClient(conn)
	stream, openStreamerr := client.StreamSnapshotRPC(context.Background())
	if openStreamerr != nil { return nil, openStreamerr }
	
	snapshotStreamChannel := make(chan []byte, 100000)

	go func() {
		snpService.ReadSnapshotContentStream(initSnapshotShotReq.SnapshotFilePath, snapshotStreamChannel)
		close(snapshotStreamChannel)
	}()

	sendSnapshotChunk := func(chunk []byte) error {
		snapshotWithChunk := &snapshotrpc.SnapshotChunk{
			LastIncludedIndex: initSnapshotShotReq.LastIncludedIndex,
			LastIncludedTerm: initSnapshotShotReq.LastIncludedTerm,
			SnapshotFilePath: initSnapshotShotReq.SnapshotFilePath,
			SnapshotChunk: chunk,
		}

		streamErr := stream.Send(snapshotWithChunk)
		if streamErr != nil { return streamErr }

		return nil
	}
	
	for chunk := range snapshotStreamChannel {
		streamErr := sendSnapshotChunk(chunk)
		if streamErr != nil { return nil, streamErr }
	}

	res, resErr := stream.CloseAndRecv()
	if resErr != nil { return nil, resErr }

	snpService.Log.Debug("result from stream received for:", sys.Host, ",returning success")
	return res, nil
}

func (snpService *SnapshotService) ReadSnapshotContentStream(snapshotFilePath string, snapshotStreamChannel chan []byte) error {
	snapshotFile, openErr := os.Open(snapshotFilePath)
	if openErr != nil { return openErr }

	defer snapshotFile.Close()

	for {
		buffer := make([]byte, ChunkSize)

		_, readErr := snapshotFile.Read(buffer)
		if readErr == io.EOF { return nil }
		if readErr != nil { return readErr }

		snapshotStreamChannel <- buffer
	}
}