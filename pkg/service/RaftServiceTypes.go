package service

import "sync"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/relay"
import "github.com/sirgallo/raft/pkg/snapshot"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"


type RaftPortOpts struct {
	LeaderElection int
	ReplicatedLog int
	Relay int
	Snapshot int
}

type RaftServiceOpts [T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State] struct {
	Protocol string
	Ports RaftPortOpts
	SystemsList []*system.System[T]
	ConnPoolOpts connpool.ConnectionPoolOpts

	StateMachine *statemachine.StateMachine[U, V, W]
	SnapshotHandler func(sm *statemachine.StateMachine[U, V, W], opts snapshot.SnapshotHandlerOpts) (*snapshotrpc.Snapshot, error)
	SnapshotReplayer func(sm *statemachine.StateMachine[U, V, W], snapshot *snapshotrpc.Snapshot) (bool, error)
}

type RaftService [T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State] struct {
	// Persistent State
	Protocol string
	Ports RaftPortOpts
	CurrentSystem *system.System[T]
	Systems *sync.Map

	LeaderElection *leaderelection.LeaderElectionService[T]
	ReplicatedLog *replog.ReplicatedLogService[T]
	Relay *relay.RelayService[T]
	Snapshot *snapshot.SnapshotService[T, U, V, W]

	CommandChannel chan T
	StateMachineLogApplyChan chan replog.LogCommitChannelEntry[T]
	StateMachineLogAppliedChan chan error

	// Volatile State
	CommitIndex int64
	LastApplied int64

	StateMachine *statemachine.StateMachine[U, V, W]
	SnapshotReplayer func(sm *statemachine.StateMachine[U, V, W], snapshot *snapshotrpc.Snapshot) (bool, error)
}


const DefaultCommitIndex = -1
const DefaultLastApplied = -1
const CommandChannelBuffSize = 100000