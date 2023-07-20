package replog


type LogEntry [T comparable] struct {
	Index int64
	Term int64
	Command T // command can be type T to represent the specific state machine commands 
}

type ReplicatedLog [T comparable] struct {
	replog []*LogEntry[T]
}