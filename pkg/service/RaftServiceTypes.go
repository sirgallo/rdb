package service

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/statemachine"


type RaftPortOpts struct {
	LeaderElection int
	ReplicatedLog  int
}

type RaftServiceOpts [T any, U string, V comparable] struct {
	Protocol      string
	Ports         RaftPortOpts
	CurrentSystem *system.System[statemachine.StateMachineOperation[U, V]]
	SystemsList   []*system.System[statemachine.StateMachineOperation[U, V]]
	ConnPoolOpts  connpool.ConnectionPoolOpts

	StateMachineClientConstructor statemachine.StateMachineClientConstructor[T]
	StateMachineClient statemachine.StateMachineClient[T, U, V]
}

type RaftService [T any, U string, V comparable] struct {
	// Persistent State
	Protocol       string
	Ports          RaftPortOpts
	CurrentSystem  *system.System[statemachine.StateMachineOperation[U, V]]
	SystemsList    []*system.System[statemachine.StateMachineOperation[U, V]]

	LeaderElection *leaderelection.LeaderElectionService[statemachine.StateMachineOperation[U, V]]
	ReplicatedLog  *replog.ReplicatedLogService[statemachine.StateMachineOperation[U, V]]

	// Volatile State
	CommitIndex    int64
	LastApplied    int64

	StateMachineOperations statemachine.StateMachineOperationMap[U, V]

	ClientCommandChannel chan statemachine.StateMachineOperation[U, V]
}