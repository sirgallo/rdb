package wal

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Write Ahead Log Snapshot Ops


/*
	Set Snapshot
		Set the snapshot entry for the latest snapshot
		--> the entry contains a reference to the file path where the latest snapshot is stored on disk
*/

func (wal *WAL) SetSnapshot(snapshot *SnapshotEntry) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Snapshot)
		bucket := tx.Bucket(bucketName)

		key := []byte(SnapshotKey)

		value, transformErr := utils.EncodeStructToBytes[*SnapshotEntry](snapshot)
		if transformErr != nil { return transformErr }

		putErr := bucket.Put(key, value)
		if putErr != nil { return putErr }

		return nil
	}

	setErr := wal.DB.Update(transaction)
	if setErr != nil { return setErr }

	return nil
}

/*
	Get Snapshot
		Get the latest snapshot entry to get the path to the latest snapshot
*/

func (wal *WAL) GetSnapshot() (*SnapshotEntry, error) {
	var snapshotEntry *SnapshotEntry
	
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Snapshot)
		bucket := tx.Bucket(bucketName)

		key := []byte(SnapshotKey)

		val := bucket.Get(key)
		if val != nil {
			decoded, decodeErr := utils.DecodeBytesToStruct[SnapshotEntry](val)
			if decodeErr != nil { return decodeErr }

			snapshotEntry = decoded
		}

		return nil
	}

	getErr := wal.DB.View(transaction)
	if getErr != nil { return nil, getErr }

	return snapshotEntry, nil
}