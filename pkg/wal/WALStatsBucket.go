package wal

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/stats"


//=========================================== Write Ahead Log Stats Ops


/*
	Set Stat
		set the latest statistic in the time series
			--> keys are iso strings, so keys are stored earliest to latest
*/

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

/*
	Get Stats
		Get the current stats in the time series
*/

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

/*
	Delete Stats
		stats in the time series are limited to a fixed size, so when the size limit is hit,
		delete the earliest keys up to the limit
*/

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