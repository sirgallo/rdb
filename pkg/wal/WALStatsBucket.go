package wal

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/stats"


//=========================================== Write Ahead Log Snapshot Ops


func (wal *WAL) SetStat(statObj stats.Stats) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Stats)
		bucket := tx.Bucket(bucketName)

		key := []byte(statObj.Timestamp)

		value, encErr := stats.EncodeStatObjectToBytes(statObj)
		if encErr != nil { return encErr}

		putErr := bucket.Put(key, value)
		if putErr != nil { return putErr }

		return nil
	}

	setErr := wal.DB.Update(transaction)
	if setErr != nil { return setErr }

	return nil
}

func (wal *WAL) GetStats() ([]stats.Stats, error) {
	var statsArr []stats.Stats
	
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Stats)
		bucket := tx.Bucket(bucketName)

		cursor := bucket.Cursor()

		for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
			if val != nil {
				statObj, decErr := stats.DecodeBytesToStatObject(val)
				if decErr != nil { return decErr}

				statsArr = append(statsArr, *statObj)
			}
		}

		return nil
	}

	getErr := wal.DB.View(transaction)
	if getErr != nil { return nil, getErr }

	return statsArr, nil
}

func (wal *WAL) DeleteStats() error {	
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Stats)
		bucket := tx.Bucket(bucketName)
		
		cursor := bucket.Cursor()

		keyCount := 0
		
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			keyCount++
		}

		numKeysToDelete := keyCount - MaxStats
		totalDeleted := 0
		
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			if  totalDeleted <= numKeysToDelete {
				bucket.Delete(key)
				totalDeleted++
			}
		}

		return nil
	}

	deleteErr := wal.DB.Update(transaction)
	if deleteErr != nil { return deleteErr }

	return nil
}