package snapshot

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"


type SnapshotServiceOpts struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System
	Systems *sync.Map
}

type SnapshotService struct {
	snapshotrpc.UnimplementedSnapshotServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System
	Systems *sync.Map

	AttemptSnapshotTimer *time.Timer

	SnapshotStartSignal chan bool
	UpdateSnapshotForSystemSignal chan string
	ProcessIncomingSnapshotSignal chan *snapshotrpc.SnapshotChunk

	Log clog.CustomLog
}

const NAME = "Snapshot"
const RPCTimeout = 200 * time.Millisecond
const AttemptSnapshotInterval = 1 * time.Minute
const SnapshotTriggerAppliedIndex = 10000

const ChunkSize = 1000000	// we will stream 1MB chunks
const FractionOfAvailableSizeToTake = 1000 // let's take consistent snapshots