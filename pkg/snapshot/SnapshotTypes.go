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

	SnapshotStartSignal chan bool
	SnapshotCompleteSignal chan bool
	UpdateSnapshotForSystemSignal chan string

	Log clog.CustomLog
}

const NAME = "Snapshot"
const RPCTimeout = 200 * time.Millisecond
const SnapshotTriggerAppliedIndex = 10000