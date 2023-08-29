package service

import "sync"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"


type RaftPortOpts struct {
	LeaderElection int
	ReplicatedLog  int
}

type RaftServiceOpts [T comparable] struct {
	Protocol      string
	SystemsList   []*system.System[T]
	ConnPoolOpts  connpool.ConnectionPoolOpts
}

type RaftService [T comparable] struct {
	// Persistent State
	Protocol       string
	Ports          RaftPortOpts
	CurrentSystem  *system.System[T]
	Systems    		 *sync.Map

	LeaderElection *leaderelection.LeaderElectionService[T]
	ReplicatedLog  *replog.ReplicatedLogService[T]

	// Volatile State
	CommitIndex    int64
	LastApplied    int64
}