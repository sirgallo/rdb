package wal

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Write Ahead Log Snapshot Ops


func (wal *WAL[T]) SetSnapshot(snapshot *snapshotrpc.Snapshot) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Snapshot)
		bucket := tx.Bucket(bucketName)

		key := []byte(SnapshotKey)

		value, transformErr := utils.EncodeStructToBytes[*snapshotrpc.Snapshot](snapshot)
		if transformErr != nil { return transformErr }

		putErr := bucket.Put(key, value)
		if putErr != nil { return putErr }

		return nil
	}

	setErr := wal.DB.Update(transaction)
	if setErr != nil { return setErr }

	return nil
}

func (wal *WAL[T]) GetSnapshot() (*snapshotrpc.Snapshot, error) {
	var snapshot *snapshotrpc.Snapshot
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Snapshot)
		bucket := tx.Bucket(bucketName)

		key := []byte(SnapshotKey)

		val := bucket.Get(key)
		
		if val != nil {
			decoded, transformErr := utils.DecodeBytesToStruct[*snapshotrpc.Snapshot](val)
			if transformErr != nil { return transformErr }

			snapshot = *decoded
		}

		return nil
	}

	getErr := wal.DB.View(transaction)
	if getErr != nil { return nil, getErr }

	return snapshot, nil
}