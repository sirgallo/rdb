package statemachine


type Action = string
type Data = comparable
type State = comparable

type StateMachineOperation [T Action, U Data] struct {
	Action T
	Data U
}

type StateMachine [T Action, U Data, V State] struct {
	State V
	Ops map [T] func (operation StateMachineOperation[T, V]) (V, error)
}