package log

type MachineCommands = comparable

type LogEntry [T MachineCommands] struct {
	Index int64
	Term int64
	Command T // command can be type T to represent the specific state machine commands 
}