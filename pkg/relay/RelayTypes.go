package relay

import "sync"

import "github.com/sirgallo/raft/pkg/relayrpc"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/system"


type RelayOpts [T system.MachineCommands] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System[T]
	SystemsList []*system.System[T]
	Systems *sync.Map
}

type RelayService [T system.MachineCommands] struct {
	relayrpc.UnimplementedRelayServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	Systems *sync.Map

	// Module Level State
	RelayChannel chan T
	RelayedAppendLogSignal chan T

	Log clog.CustomLog
}