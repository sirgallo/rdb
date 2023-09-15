package system

import "sync"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/wal"


type SystemState string
type SystemStatus int

type LogEntry [T log.MachineCommands] struct {
	Index int64
	Term int64
	Command T // command can be type T to represent the specific state machine commands 
}

type System [T log.MachineCommands] struct {
	Host string
	Status SystemStatus
	
	State SystemState
	CurrentTerm int64
	CommitIndex int64
	LastApplied int64
	VotedFor string
	CurrentLeader string

	WAL *wal.WAL[T]

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