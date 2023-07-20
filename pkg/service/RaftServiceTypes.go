package service

import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/shared"


type RaftServiceOpts struct {
	Protocol string
	Port     int
}

type RaftService[T comparable] struct {
	// Persistent State
	CurrentTerm   *int64
	CurrentSystem *shared.System
	SystemList    []*shared.System

	LeaderElection *leaderelection.LeaderElectionService
	// Log replog.ReplicatedLog[T]

	// Volatile State
	CommitIndex int64
	LastApplied int64

	// Volatile State On Leader
	// NextIndex
}
