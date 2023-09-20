package forwardresp

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/forwardresprpc"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"


type ForwardRespOpts struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System
	SystemsList []*system.System
	Systems *sync.Map
}

type ForwardRespService struct {
	forwardresprpc.UnimplementedForwardRespServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool
	Mutex sync.Mutex

	CurrentSystem *system.System
	Systems *sync.Map

	LeaderRelayResponseChannel chan statemachine.StateMachineResponse
	ForwardRespChannel chan statemachine.StateMachineResponse

	Log clog.CustomLog
}

const NAME = "Forward Response"
const RPCTimeout = 50 * time.Millisecond

const RelayRespBuffSize = 100000
const ForwardRespChannel = 100000