package snapshot

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"


type SnapshotHandlerOpts struct {
	LastIncludedIndex int64
	LastIncludedTerm int64
}

type SnapshotServiceOpts [T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System[T]
	Systems *sync.Map

	StateMachine *statemachine.StateMachine[U, V, W]
	SnapshotHandler func(sm *statemachine.StateMachine[U, V, W], opts SnapshotHandlerOpts) (*snapshotrpc.Snapshot, error)
}

type SnapshotService [T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State] struct {
	snapshotrpc.UnimplementedSnapshotServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System[T]
	Systems *sync.Map

	StateMachine *statemachine.StateMachine[U, V, W]
	SnapshotStartSignal chan bool
	SnapshotCompleteSignal chan bool
	UpdateSnapshotForSystemSignal chan string
	SnapshotHandler func(sm *statemachine.StateMachine[U, V, W], opts SnapshotHandlerOpts) (*snapshotrpc.Snapshot, error)
	// SnapshotReplayer func(sm *statemachine.StateMachine[U, V, W]) (bool, error)

	Log clog.CustomLog
}

const NAME = "Snapshot"
const RPCTimeout = 200 * time.Millisecond
const SnapshotTriggerAppliedIndex = 10000