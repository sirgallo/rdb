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
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)

		key := ConvertIntToBytes(index)

		value, transformErr := log.TransformLogEntryToBytes[T](entry)
		if transformErr != nil { return transformErr }

		putErr := walBucket.Put(key, value)
		if putErr != nil { return putErr }

		totalBytesAdded := int64(len(key)) + int64(len(value))
		totalKeysAdded := int64(1)

		sizeKey := []byte(ReplogSizeKey)
		totalKey := []byte(ReplogTotalElementsKey)

		var bucketSize, totalKeys int64
		
		sizeVal := statsBucket.Get(sizeKey)
		if sizeVal == nil {
			bucketSize = 0
		} else { bucketSize = ConvertBytesToInt(sizeVal) }

		totalVal := statsBucket.Get(totalKey)
		if totalVal == nil {
			totalKeys = 0
		} else { totalKeys = ConvertBytesToInt(totalVal) }

		newBucketSize := bucketSize + totalBytesAdded
		newTotal := totalKeys + totalKeysAdded

		putSizeErr := statsBucket.Put(sizeKey, ConvertIntToBytes(newBucketSize))
		if putSizeErr != nil { return putSizeErr }

		putTotalErr := statsBucket.Put(totalKey, ConvertIntToBytes(newTotal))
		if putTotalErr != nil { return putTotalErr }

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
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)

		totalBytesAdded := int64(0)
		totalKeysAdded := int64(1)

		for _, currLog := range logs {
			key := ConvertIntToBytes(currLog.Index)
		
			newVal, transformErr := log.TransformLogEntryToBytes[T](currLog)
			if transformErr != nil { return transformErr }
	
			putErr := walBucket.Put(key, newVal)
			if putErr != nil { return putErr }

			totalBytesAdded += int64(len(key)) + int64(len(newVal))
			totalKeysAdded++
		}

		sizeKey := []byte(ReplogSizeKey)
		totalKey := []byte(ReplogTotalElementsKey)

		var bucketSize, totalKeys int64
		
		sizeVal := statsBucket.Get(sizeKey)
		if sizeVal == nil {
			bucketSize = 0
		} else { bucketSize = ConvertBytesToInt(sizeVal) }

		totalVal := statsBucket.Get(totalKey)
		if totalVal == nil {
			totalKeys = 0
		} else { totalKeys = ConvertBytesToInt(totalVal) }

		newBucketSize := bucketSize + totalBytesAdded
		newTotal := totalKeys + totalKeysAdded

		putSizeErr := statsBucket.Put(sizeKey, ConvertIntToBytes(newBucketSize))
		if putSizeErr != nil { return putSizeErr }

		putTotalErr := statsBucket.Put(totalKey, ConvertIntToBytes(newTotal))
		if putTotalErr != nil { return putTotalErr }

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
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		key := ConvertIntToBytes(index)

		val := walBucket.Get(key)
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

		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		startKey := ConvertIntToBytes(startIndex)
		endKey := ConvertIntToBytes(endIndex)

		cursor := walBucket.Cursor()

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
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		cursor := walBucket.Cursor()
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
	Get Earliest
		create a read transaction for getting the latest entry in the bucket
			1.) get the current bucket
			2.) create a cursor for the bucket and point at the first element in the bucket
			3.) transform the value from byte array to entry and return the entry
*/
func (wal *WAL[T]) GetEarliest() (*log.LogEntry[T], error) {
	var earliestLog *log.LogEntry[T]

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		cursor := walBucket.Cursor()
		_, val := cursor.First()
		
		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry[T](val)
			if transformErr != nil { return transformErr }
	
			earliestLog = entry
		} else { earliestLog = nil }

		return nil
	}
	
	readErr := wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return earliestLog, nil
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
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)
		
		key := []byte(ReplogTotalElementsKey)
		val := statsBucket.Get(key)
		
		totalKeys = int(ConvertBytesToInt(val))

		return nil
	}

	readErr := wal.DB.View(transaction)
	if readErr != nil { return 0, readErr }

	return totalKeys, nil
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

func (wal *WAL[T]) GetBucketSizeInBytes() (int64, error) {
	totalSize := int64(0)

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)
		
		key := []byte(ReplogSizeKey)
		val := statsBucket.Get(key)
		
		totalSize =ConvertBytesToInt(val)

		return nil
	}

	getSizeErr := wal.DB.View(transaction)
	if getSizeErr != nil { return 0, getSizeErr }

	return totalSize, nil
}

func (wal *WAL[T]) DeleteLogs(endIndex int64) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)

		endKey := ConvertIntToBytes(endIndex)

		cursor := walBucket.Cursor()
		
		totalBytesRemoved := int64(0)
		totalKeysRemoved := int64(0)
		
		for key, val := cursor.First(); key != nil && bytes.Compare(key, endKey) <= 0; key, val = cursor.Next() {
			delErr := bucket.Delete(key)
			if delErr != nil { return delErr }

			totalBytesRemoved += int64(len(key)) + int64(len(val))
			totalKeysRemoved++
		}

		sizeKey := []byte(ReplogSizeKey)
		totalKey := []byte(ReplogTotalElementsKey)

		var bucketSize, totalKeys int64
		
		sizeVal := statsBucket.Get(sizeKey)
		if sizeVal == nil {
			bucketSize = 0
		} else { bucketSize = ConvertBytesToInt(sizeVal) }

		totalVal := statsBucket.Get(totalKey)
		if totalVal == nil {
			totalKeys = 0
		} else { totalKeys = ConvertBytesToInt(totalVal) }

		newBucketSize := bucketSize - totalBytesRemoved
		newTotal := totalKeys - totalKeysRemoved

		putSizeErr := statsBucket.Put(sizeKey, ConvertIntToBytes(newBucketSize))
		if putSizeErr != nil { return putSizeErr }

		putTotalErr := statsBucket.Put(totalKey, ConvertIntToBytes(newTotal))
		if putTotalErr != nil { return putTotalErr }

		return nil
	}

	delErr := wal.DB.Update(transaction)
	if delErr != nil { return delErr }

	return nil
}