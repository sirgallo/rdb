package service

import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"


type RaftServiceOpts struct {
	Protocol string
	Port     int
}

type RaftService [T comparable] struct {
	// Persistent State
	CurrentTerm   *int64
	CurrentSystem *system.System[T]
	SystemList    []*system.System[T]

	LeaderElection *leaderelection.LeaderElectionService[T]
	ReplicatedLog  *replog.ReplicatedLogService[T]

	// Volatile State
	CommitIndex int64
	LastApplied int64

	// Volatile State On Leader
	// NextIndex
}