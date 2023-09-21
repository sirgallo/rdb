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

func (wal *WAL) Append(entry *log.LogEntry) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		latestIndexedLog, getIndexErr := wal.getLatestIndexedEntry(bucket)
		if getIndexErr != nil { return getIndexErr}

		totalBytesAdded, totalKeysAdded, appendErr := wal.appendHelper(bucket, entry)
		if appendErr != nil { return appendErr }

		updateErr := wal.UpdateReplogStats(bucket, totalBytesAdded, totalKeysAdded, ADD)
		if updateErr != nil { return updateErr }

		_, setIndexErr := wal.setIndexForFirstLogInTerm(bucket, entry, latestIndexedLog)
		if setIndexErr != nil { return setIndexErr }

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

func (wal *WAL) RangeAppend(logs []*log.LogEntry) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		latestIndexedLog, getIndexErr := wal.getLatestIndexedEntry(bucket)
		if getIndexErr != nil { return getIndexErr}

		totalBytesAdded := int64(0)
		totalKeysAdded := int64(0)

		for _, currLog := range logs {
			entrySize, keyToAdd, appendErr := wal.appendHelper(bucket, currLog)
			if appendErr != nil { return appendErr }
			
			totalBytesAdded += entrySize
			totalKeysAdded += keyToAdd

			newIndexedEntry, setIndexErr := wal.setIndexForFirstLogInTerm(bucket, currLog, latestIndexedLog)
			if setIndexErr != nil { return setIndexErr }
			if newIndexedEntry != nil { latestIndexedLog = newIndexedEntry }
		}

		updateErr := wal.UpdateReplogStats(bucket, totalBytesAdded, totalKeysAdded, ADD)
		if updateErr != nil { return updateErr }

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

func (wal *WAL) Read(index int64) (*log.LogEntry, error) {
	var entry *log.LogEntry
	
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		key := ConvertIntToBytes(index)

		val := walBucket.Get(key)
		if val == nil { return nil }

		incoming, transformErr := log.TransformBytesToLogEntry(val)
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

func (wal *WAL) GetRange(startIndex int64, endIndex int64) ([]*log.LogEntry, error) {
	var entries []*log.LogEntry

	transaction := func(tx *bolt.Tx) error {
		transformAndAppend := func(val []byte) error {
			entry, transformErr := log.TransformBytesToLogEntry(val)
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

func (wal *WAL) GetLatest() (*log.LogEntry, error) {
	var latestEntry *log.LogEntry

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		cursor := walBucket.Cursor()
		_, val := cursor.Last()
		
		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry(val)
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

func (wal *WAL) GetEarliest() (*log.LogEntry, error) {
	var earliestLog *log.LogEntry

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

		cursor := walBucket.Cursor()
		_, val := cursor.First()
		
		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry(val)
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
	Delete Logs
		create a read-write transaction for log compaction in the bucket
			1.) for all logs up tothe last index, delete the key-value pair
			2.) for each deleted, update the total keys and the total space removed from the log
			3.) get latest log (last applied), and remove all indexes up to it
*/

func (wal *WAL) DeleteLogs(endIndex int64) error {
	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		walBucketName := []byte(ReplogWAL)
		walBucket := bucket.Bucket(walBucketName)

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

		keyOfFirstLog := ConvertIntToBytes(endIndex + 1)
		val := walBucket.Get(keyOfFirstLog)

		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry(val)
			if transformErr != nil { return transformErr }
	
			indexBucketName := []byte(ReplogIndex)
			indexBucket := bucket.Bucket(indexBucketName)

			latestTermKey := ConvertIntToBytes(entry.Term)

			indexCursor := indexBucket.Cursor()

			for key, _ := indexCursor.First(); key != nil && bytes.Compare(key, latestTermKey) < 0; key, _ = indexCursor.Next() {
				delErr := indexBucket.Delete(key)
				if delErr != nil { return delErr }
			}
		} 

		updateErr := wal.UpdateReplogStats(bucket, totalBytesRemoved, totalKeysRemoved, SUB)
		if updateErr != nil { return updateErr }
		
		return nil
	}

	delErr := wal.DB.Update(transaction)
	if delErr != nil { return delErr }

	return nil
}

/*
	Get Total
		create a read transaction for getting total keys in the bucket
			1.) read from the stats bucket and check the indexed total value
*/

func (wal *WAL) GetTotal() (int, error) {
	totalKeys := 0

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)
		
		key := []byte(ReplogTotalElementsKey)
		val := statsBucket.Get(key)
		if val != nil { totalKeys = int(ConvertBytesToInt(val)) }

		return nil
	}

	readErr := wal.DB.View(transaction)
	if readErr != nil { return 0, readErr }

	return totalKeys, nil
}

/*
	Get Bucket Size In Bytes
		create a read transaction for getting total size of the replicated log in bytes
			1.) read from the stats bucket and check the indexed total size
*/

func (wal *WAL) GetBucketSizeInBytes() (int64, error) {
	totalSize := int64(0)

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		statsBucketName := []byte(ReplogStats)
		statsBucket := bucket.Bucket(statsBucketName)
		
		key := []byte(ReplogSizeKey)
		val := statsBucket.Get(key)
		if val != nil { totalSize = ConvertBytesToInt(val) }

		return nil
	}

	getSizeErr := wal.DB.View(transaction)
	if getSizeErr != nil { return 0, getSizeErr }

	return totalSize, nil
}

/*
	Update Replog Stats
		helper function for updating both the indexes for total keys and total size of the replicated log
*/

func (wal *WAL) UpdateReplogStats(bucket *bolt.Bucket, numUpdatedBytes int64, numUpdatedKeys int64, op StatOP) error {
	statsBucketName := []byte(ReplogStats)
	statsBucket := bucket.Bucket(statsBucketName)
	
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

	var newBucketSize, newTotal int64

	if op == ADD {
		newBucketSize = bucketSize + numUpdatedBytes
		newTotal = totalKeys + numUpdatedKeys
	} else if op == SUB {
		newBucketSize = bucketSize - numUpdatedBytes
		newTotal = totalKeys - numUpdatedKeys
	}

	putSizeErr := statsBucket.Put(sizeKey, ConvertIntToBytes(newBucketSize))
	if putSizeErr != nil { return putSizeErr }

	putTotalErr := statsBucket.Put(totalKey, ConvertIntToBytes(newTotal))
	if putTotalErr != nil { return putTotalErr }

	return nil
}

/*
	Get Indexed Entry For Term
		For the given term, check the indexed value and to get the earliest known entry
*/

func (wal *WAL) GetIndexedEntryForTerm(term int64) (*log.LogEntry, error) {
	var indexedEntry *log.LogEntry

	transaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		bucket := tx.Bucket(bucketName)

		indexBucketName := []byte(ReplogIndex)
		indexBucket := bucket.Bucket(indexBucketName)
		
		key := ConvertIntToBytes(term)
		val := indexBucket.Get(key)
		if val != nil { 
			entry, transformErr := log.TransformBytesToLogEntry(val)
			if transformErr != nil { return transformErr }
	
			indexedEntry = entry
		} else { indexedEntry = nil }

		return nil
	}

	getIndexErr := wal.DB.View(transaction)
	if getIndexErr != nil { return nil, getIndexErr }

	return indexedEntry, nil
}

/*
	Append Helper
		shared function for appending entries to the replicated log
*/

func (wal *WAL) appendHelper(bucket *bolt.Bucket, entry *log.LogEntry) (int64, int64, error) {
	walBucketName := []byte(ReplogWAL)
	walBucket := bucket.Bucket(walBucketName)

	key := ConvertIntToBytes(entry.Index)

	value, transformErr := log.TransformLogEntryToBytes(entry)
	if transformErr != nil { return 0, 0, transformErr }

	putErr := walBucket.Put(key, value)
	if putErr != nil { return 0, 0, putErr }

	totalBytesAdded := int64(len(key)) + int64(len(value))
	totalKeysAdded := int64(1)

	return totalBytesAdded, totalKeysAdded, nil
}

/*
	Get Latest Indexed Entry
		get the earliest known entry for the latest term known in the cluster
*/

func (wal *WAL) getLatestIndexedEntry(bucket *bolt.Bucket) (*log.LogEntry, error) {
	indexBucketName := []byte(ReplogIndex)
	indexBucket := bucket.Bucket(indexBucketName)

	cursor := indexBucket.Cursor()
	_, val := cursor.Last()
	if val == nil { 
		return &log.LogEntry{
			Index: 0,
			Term: 0,
		}, nil 
	}

	entry, transformErr := log.TransformBytesToLogEntry(val)
	if transformErr != nil { return nil, transformErr }

	return entry, nil
}

/*
	When a higher term than previously known is discovered, update the index to include the first entry associated with term
*/

func (wal *WAL) setIndexForFirstLogInTerm(bucket *bolt.Bucket, newEntry *log.LogEntry, previousIndexed *log.LogEntry) (*log.LogEntry, error) {
	if newEntry.Term > previousIndexed.Term {
		indexBucketName := []byte(ReplogIndex)
		indexBucket := bucket.Bucket(indexBucketName)

		key := ConvertIntToBytes(newEntry.Term)
		
		entryAsBytes, transformErr := log.TransformLogEntryToBytes(newEntry)
		if transformErr != nil { return nil, transformErr }
		
		setErr := indexBucket.Put(key, entryAsBytes)
		if setErr != nil { return nil, setErr }

		return newEntry, nil
	}

	return nil, nil
}