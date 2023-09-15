package wal

import "bytes"
import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/log"


func (wal *WAL[T]) Append(index int64, entry *log.LogEntry[T]) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		key := ConvertIntToBytes(index)

		value, transformErr := log.TransformLogEntryToBytes[T](entry)
		if transformErr != nil { return transformErr }

		putErr := bucket.Put(key, value)
		if putErr != nil { return putErr }

		return nil
	}

	appendErr := wal.DB.Update(transaction)
	if appendErr != nil { return appendErr }

	return nil
}

func (wal *WAL[T]) RangeUpdate(logs []*log.LogEntry[T]) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	for _, currLog := range logs {
		transaction := func(tx *bolt.Tx) error {
			bucketName := []byte(Bucket)
			bucket := tx.Bucket(bucketName)
	
			key := ConvertIntToBytes(currLog.Index)
			
			newVal, transformErr := log.TransformLogEntryToBytes[T](currLog)
			if transformErr != nil { return transformErr }
	
			putErr := bucket.Put(key, newVal)
			if putErr != nil { return putErr }
	
			return nil
		}

		rangeUpdateErr := wal.DB.Update(transaction)
		if rangeUpdateErr != nil { return rangeUpdateErr }
	}

	return nil
}

func (wal *WAL[T]) Read(index int64) (*log.LogEntry[T], error) {
	var entry *log.LogEntry[T]
	
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		key := ConvertIntToBytes(index)

		val := bucket.Get(key)
		if val == nil { return nil }

		incoming, transformErr := log.TransformBytesToLogEntry[T](val)
		if transformErr != nil { return transformErr }

		entry = incoming
		
		return nil
	}

	readErr := wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return entry, nil
}

func (wal *WAL[T]) GetRange(startIndex int64, endIndex int64) ([]*log.LogEntry[T], error) {
	var entries []*log.LogEntry[T]

	transaction := func(tx *bolt.Tx) error {
		transformAndAppend := func(val []byte) error {
			entry, transformErr := log.TransformBytesToLogEntry[T](val)
			if transformErr != nil { return transformErr }
			
			entries = append(entries, entry)
	
			return nil
		}

		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		startKey := ConvertIntToBytes(startIndex)
		endKey := ConvertIntToBytes(endIndex)

		cursor := bucket.Cursor()

		for key, val := cursor.Seek(startKey); key != nil && bytes.Compare(key, endKey) <= 0; key, val = cursor.Next() {
			if val != nil { transformAndAppend(val) }
		}

		return nil
	}

	readErr := wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return entries, nil
}

func (wal *WAL[T]) GetLatest() (*log.LogEntry[T], error) {
	var latestEntry *log.LogEntry[T]

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		cursor := bucket.Cursor()
		_, val := cursor.Last()
		
		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry[T](val)
			if transformErr != nil { return transformErr }
	
			latestEntry = entry
		} else { latestEntry = nil }

		return nil
	}
	
	readErr := wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return latestEntry, nil
}

func (wal *WAL[T]) GetTotal(startIndex int64, endIndex int64) (int, error) {
	totalKeys := 0
	if startIndex >= endIndex { return totalKeys, nil }

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		cursor := bucket.Cursor()
		
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			totalKeys++
		}

		return nil
	}

	readErr := wal.DB.View(transaction)
	if readErr != nil { return 0, readErr }

	return totalKeys, nil
}