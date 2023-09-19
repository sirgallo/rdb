package statemachine

import "sync"

import bolt "go.etcd.io/bbolt"


type Action = string

type StateMachineOpPayload struct {
	Collection string
	Value	string
}

type StateMachineOperation struct {
	Action Action
	Payload StateMachineOpPayload
}

type StateMachineResponse struct {
	Collection string
	Key string
	Value string
}

type StateMachine struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}


const NAME = "StateMachine"
const SubDirectory = "raft/statemachine"
const FileName = "statemachine.db"

const (
	FIND Action = "find"
	INSERT Action = "insert"
	DELETE Action = "delete"
	CREATECOLLECTION Action = "create collection"
	DROPCOLLECTION Action = "drop collection"
	LISTCOLLECTIONS Action = "list collections"
	RANGE Action = "range"
)

const RootBucket = "root"
const CollectionBucket = "collection"
const IndexBucket = "index"

const IndexSuffix = "_index"
