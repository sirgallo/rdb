package statemachine

import "sync"


type Action = string
type Data = comparable
type State = comparable

type StateMachineOperation [T Action, U Data] struct {
	Action T
	Data U
}

type StateMachine [T Action, U Data, V State] struct {
	Mutex sync.Mutex
	State V
	Ops func(operation StateMachineOperation[T, U]) (U, error)
}