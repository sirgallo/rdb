package leaderelection

import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"


type LeaderElectionOpts [T system.MachineCommands] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System[T]
	SystemsList   []*system.System[T]
}

type LeaderElectionService [T system.MachineCommands] struct {
	lerpc.UnimplementedLeaderElectionServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	SystemsList   []*system.System[T]

	// Module Level State
	VotedFor string
	Timeout  time.Duration

	ResetTimeoutSignal chan bool
}