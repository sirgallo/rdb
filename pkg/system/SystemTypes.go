package system


type SystemState string
type SystemStatus int
type MachineCommands = comparable

type LogEntry [T MachineCommands] struct {
	Index   int64
	Term    int64
	Command T // command can be type T to represent the specific state machine commands 
}

type System [T MachineCommands] struct {
	Host        string
	Status			SystemStatus
	
	State       SystemState
	CurrentTerm int64
	CommitIndex int64
	LastApplied int64
	VotedFor		string

	Replog      []*LogEntry[T]

	NextIndex  int64 // next index to send to a server
}

type StateTransitionOpts struct {
	CurrentTerm *int64
	VotedFor		*string
} 

const (
	Leader    SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower  SystemState = "follower"
)

const (
	Dead  SystemStatus = 0
	Alive SystemStatus = 1
)