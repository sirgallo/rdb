package keyvalstore

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshot"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/utils"


const NAME = "Key Val Snapshot"
var Log = clog.NewCustomLog(NAME)


func SnapshotKeyValStore(sm *KeyValStateMachine, opts snapshot.SnapshotHandlerOpts) (*snapshotrpc.Snapshot, error) {
	var keyValPairs []string

	sm.State.Range(func(key, val interface{}) bool {
		keyValPair := KeyValPair{
			Key: key.(string),
			Value: val.(string),
		}

		kvPair, err := utils.EncodeStructToString[KeyValPair](keyValPair)
		if err != nil { Log.Error("error encoding log struct to string") }

		keyValPairs = append(keyValPairs, kvPair)

		return true
	})

	return &snapshotrpc.Snapshot{
		LastIncludedIndex: opts.LastIncludedIndex,
		LastIncludedTerm: opts.LastIncludedTerm,
		StateMachineState: keyValPairs,
	}, nil
}

func SnapshotKeyValStoreReplay(sm *KeyValStateMachine, snapshot *snapshotrpc.Snapshot) (bool, error) {
	for _, strEntry := range snapshot.StateMachineState {
		kvPair, decErr := utils.DecodeStringToStruct[KeyValPair](strEntry)
		if decErr != nil { return false, decErr }

		sm.State.Store(kvPair.Key, kvPair.Value)
	}

	return true, nil
}