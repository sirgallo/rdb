package statemachine

import "sync"

import bolt "go.etcd.io/bbolt"


type Action = string

type StateMachineOpPayload struct {
	Collection string `json:"collection"`
	Value	string `json:"value"`
}

type StateMachineOperation struct {
	RequestID string `json:"-"`
	// RequestOrigin string `json:"-"`
	Action Action `json:"action"`
	Payload StateMachineOpPayload `json:"payload"`
}

type StateMachineResponse struct {
	RequestID string `json:"-"`
	// RequestOrigin string `json:"-"`
	Collection string `json:"collection"`
	Key string `json:"key"`
	Value string `json:"value"`
}

type StateMachine struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}


const NAME = "StateMachine"
const SubDirectory = "raft/statemachine"
const FileNamePrefix = "statemachine"
const DbFileName = FileNamePrefix + ".db"

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
