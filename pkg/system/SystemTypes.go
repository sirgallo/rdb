package system

import "sync"

import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/wal"


type SystemState string
type SystemStatus int

type System struct {
	Host string
	Status SystemStatus
	
	State SystemState
	CurrentTerm int64
	CommitIndex int64
	LastApplied int64
	VotedFor string
	CurrentLeader string

	WAL *wal.WAL
	StateMachine *statemachine.StateMachine

	NextIndex int64 // next index to send to a server

	SystemMutex sync.Mutex
}

type StateTransitionOpts struct {
	CurrentTerm *int64
	VotedFor *string
} 

const (
	Leader SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower SystemState = "follower"
)

const (
	Dead SystemStatus = 0
	Ready SystemStatus = 1
	Busy SystemStatus = 2
)

const DefaultLastLogIndex = -1 // -1 symbolizes empty log
const DefaultLastLogTerm = 0