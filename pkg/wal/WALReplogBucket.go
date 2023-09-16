package wal

import "bytes"
import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/log"


//=========================================== Write Ahead Log Replog Bucket Ops


/*
	Append
		create a read-write transaction for the bucket to append a single new entry
			1.) get the current bucket
			2.) transform the entry and key to byte arrays
			3.) put the key and value in the bucket
*/

func (wal *WAL[T]) Append(index int64, entry *log.LogEntry[T]) error {
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

/*
	Range Append
		create a read-write transaction for the bucket to append a set of new entries
			1.) get the current bucket
			2.) iterate over the new entries and perform the same as single Append
*/

func (wal *WAL[T]) RangeAppend(logs []*log.LogEntry[T]) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		for _, currLog := range logs {
			key := ConvertIntToBytes(currLog.Index)
		
			newVal, transformErr := log.TransformLogEntryToBytes[T](currLog)
			if transformErr != nil { return transformErr }
	
			putErr := bucket.Put(key, newVal)
			if putErr != nil { return putErr }
		}

		return nil
	}

	rangeUpdateErr := wal.DB.Update(transaction)
	if rangeUpdateErr != nil { return rangeUpdateErr }

	return nil
}

/*
	Read
		create a read transaction for getting a single key-value entry
			1.) get the current bucket
			2.) get the value for the key as bytes
			3.) transform the byte array back to an entry and return
*/

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

/*
	Get Range
		create a read transaction for getting a range of entries
			1.) get the current bucket
			2.) create a cursor for the bucket
			3.) seek from the specified start index and iterate until end
			4.) for each value, transform from byte array to entry and append to return array
			5.) return all entries
*/

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

/*
	Get Latest
		create a read transaction for getting the latest entry in the bucket
			1.) get the current bucket
			2.) create a cursor for the bucket and point at the last element in the bucket
			3.) transform the value from byte array to entry and return the entry
*/
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

/*
	Get Total
		create a read transaction for getting total keys in the bucket
			1.) get the current bucket
			2.) create a cursor for the bucket
			3.) start from the first element in the bucket and iterate, monotonically increasing
				the total keys
			4.) return total keys
*/

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

func (wal *WAL[T]) DeleteLogs(endIndex int64) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		bucket := tx.Bucket(bucketName)

		endKey := ConvertIntToBytes(endIndex)

		cursor := bucket.Cursor()
		
		for key, _ := cursor.First(); key != nil && bytes.Compare(key, endKey) <= 0; key, _ = cursor.Next() {
			delErr := bucket.Delete(key)
			if delErr != nil { return delErr }
		}

		return nil
	}

	delErr := wal.DB.Update(transaction)
	if delErr != nil { return delErr }

	return nil
}