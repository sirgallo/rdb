package system


type SystemState string

const (
	Leader    SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower  SystemState = "follower"
)

type SystemStatus int

const (
	Dead  SystemStatus = 0
	Alive SystemStatus = 1
)

type LogEntry [T comparable] struct {
	Index   int64
	Term    int64
	Command T // command can be type T to represent the specific state machine commands 
}

type System [T comparable] struct {
	Host        string
	Status			SystemStatus
	
	State       SystemState
	CurrentTerm int64
	CommitIndex int64

	Replog []*LogEntry[T]

	NextIndex  int64 // next index to send to a server
}