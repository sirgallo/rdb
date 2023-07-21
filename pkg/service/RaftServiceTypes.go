package service

import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/shared"


type RaftServiceOpts struct {
	Protocol string
	Port     int
}

type RaftService [T comparable] struct {
	// Persistent State
	CurrentTerm   *int64
	CurrentSystem *shared.System[T]
	SystemList    []*shared.System[T]

	LeaderElection *leaderelection.LeaderElectionService[T]
	ReplicatedLog  *replog.ReplicatedLogService[T]

	// Volatile State
	CommitIndex int64
	LastApplied int64

	// Volatile State On Leader
	// NextIndex
}