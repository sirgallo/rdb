package service

import "sync"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/relay"
import "github.com/sirgallo/raft/pkg/system"


type RaftPortOpts struct {
	LeaderElection int
	ReplicatedLog int
	Relay int
}

type RaftServiceOpts [T system.MachineCommands] struct {
	Protocol string
	Ports RaftPortOpts
	SystemsList []*system.System[T]
	ConnPoolOpts connpool.ConnectionPoolOpts
}

type RaftService [T system.MachineCommands] struct {
	// Persistent State
	Protocol string
	Ports RaftPortOpts
	CurrentSystem *system.System[T]
	Systems *sync.Map

	LeaderElection *leaderelection.LeaderElectionService[T]
	ReplicatedLog *replog.ReplicatedLogService[T]
	Relay *relay.RelayService[T]

	CommandChannel chan T
	StateMachineLogApplyChan chan replog.LogCommitChannelEntry[T]
	StateMachineLogAppliedChan chan error

	// Volatile State
	CommitIndex int64
	LastApplied int64
}


const DefaultCommitIndex = -1
const DefaultLastApplied = -1
const CommandChannelBuffSize = 100000