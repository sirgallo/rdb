package replog

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/shared"


const HeartbeatIntervalInMs = 50

type LogEntry [T comparable] struct {
	Index   int64
	Term    int64
	Command T // command can be type T to represent the specific state machine commands 
}

type ReplicatedLogOpts struct {
	Port int

	CommitIndex   *int64
	CurrentTerm   *int64
	CurrentSystem *shared.System
	SystemsList   []*shared.System
}

type ReplicatedLogService [T comparable] struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port string

	// Persistent State
	CommitIndex   *int64
	CurrentTerm   *int64
	CurrentSystem *shared.System
	SystemsList   []*shared.System

	// Module Specific
	PrevLogIndex int64
  PrevLogTerm  int64

	Replog []*LogEntry[T]
}