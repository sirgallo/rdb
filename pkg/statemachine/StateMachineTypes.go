package statemachine


type StateMachineOperation [T string, V comparable] struct {
	Action T
	Data V
}

type StateMachineOperationMap [T string, V comparable] struct {
	Ops map [T] func (operation StateMachineOperation[T, V]) (V, error)
}

type StateMachineClientConstructor [T any] struct {
	StateMachineConnector T
}

type StateMachineClient [T any, U string, V comparable ] interface {
	NewStateMachineOperationMap(opts StateMachineClientConstructor[T]) *StateMachineOperationMap[U, V]
}