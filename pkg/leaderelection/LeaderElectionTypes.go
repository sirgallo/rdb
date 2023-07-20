package leaderelection

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/shared"


type SystemState string

const (
	Leader    SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower  SystemState = "follower"
)

type LeaderElectionOpts struct {
	Port int

	CurrentTerm   *int64
	LastLogIndex  *int64
	LastLogTerm   *int64
	CurrentSystem *shared.System
	SystemsList   []*shared.System
}

type LeaderElectionService struct {
	lerpc.UnimplementedLeaderElectionServiceServer
	Port string

	// Persistent State
	CurrentTerm   *int64
	LastLogIndex  *int64
	LastLogTerm   *int64
	CurrentSystem *shared.System
	SystemsList   []*shared.System

	// Module Level State
	State    SystemState
	VotedFor string
	Timeout  int

	ResetTimeoutSignal chan bool
}