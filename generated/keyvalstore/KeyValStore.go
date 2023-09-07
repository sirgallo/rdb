package keyvalstore

import "sync"

import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


type KeyValPair struct {
	Key string
	Value string
}

type KeyValOps = string
type KeyValOp = statemachine.StateMachineOperation[KeyValOps, KeyValPair]

type GetOP = KeyValOp
type SetOP = KeyValOp
type DeleteOP = KeyValOp

type KeyValStore = *sync.Map

type KeyValStateMachine = statemachine.StateMachine[KeyValOps, KeyValPair, KeyValStore]

const (
	GET KeyValOps = "GET"
	SET KeyValOps = "SET"
	DELETE KeyValOps = "DELETE"
)


func NewKeyValStore() *KeyValStateMachine {
	keyValStateMachine := &KeyValStateMachine{
		State: &sync.Map{},
	}

	ops := func(operation KeyValOp) (KeyValPair, error) {
		switch operation.Action {
			case GET: 
				val, ok := keyValStateMachine.State.Load(operation.Data.Key)
				if ok {
					return KeyValPair{ 
						Key: operation.Data.Key, 
						Value: val.(string),
					}, nil
				} else { return utils.GetZero[KeyValPair](), nil }
			case SET:
				keyValStateMachine.State.Store(operation.Data.Key, operation.Data.Value)
				return operation.Data, nil
			case DELETE:
				keyValStateMachine.State.Delete(operation.Data.Key)
				return operation.Data, nil
			default:
				return utils.GetZero[KeyValPair](), nil
		}
	}

	keyValStateMachine.Ops = ops
	return keyValStateMachine
} 