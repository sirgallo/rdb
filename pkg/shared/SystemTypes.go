package shared


type SystemState string

const (
	Leader    SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower  SystemState = "follower"
)

type LogEntry [T comparable] struct {
	Index   int64
	Term    int64
	Command T // command can be type T to represent the specific state machine commands 
}

type System [T comparable] struct {
	Host        string
	
	State       SystemState
	CurrentTerm int64
	CommitIndex int64

	Replog []*LogEntry[T]
}