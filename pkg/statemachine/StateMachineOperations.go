package statemachine

import bolt "go.etcd.io/bbolt"
import "strings"

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== State Machine Operations


/*
	Bulk Apply
		operation to apply logs to the state machine and perform operations on it
			--> the process of applying log entries from the replicated log performs the operation on the state machine,
				which will also return a response back to the client that issued the command included in the log entry
			
		The operation is a struct which contains both the operation to perform and the payload included
			--> the payload includes the collection to be operated on as well as the value to update

		FIND
			perform a lookup on a value. The key does not need to be known, and the value to look for is passed in the payload
			--> the value is indexed in a separate collection, which points to the key that is associated with the value. Keys are dynamically 
					generated on inserts and do not need be known to the user. The key can be seen more as a unique identifier

		INSERT
			perform an insert for a value in a collection
			--> on inserts, first a hash is generated as the key for the value in the collection. Then, values are inserted into appropriate
				indexes. Since BoltDb utilizes a B+ tree as its primary data structure, key-value pairs are sorts by default. We can utilize this to
				create indexes for our collections, where values become the primary key and the value becomes the id of the object in the collection,
				so essentially we can point directly to the location in the collection from a given index
		
		DELETE
			perform a delete for a value in a collection
			--> this involes first doing a lookup on the index for the object to be deleted, and then removing both the original element from the
			collection and all associated indexes

		DROP COLLECTION
			perform a collection drop
			--> pass the collection to be dropped, and it will be removed from the root database bucket. All associated indexes are removed and 
			the reference to the names of the collection and indexes are removed from the collection and index buckets in root
		
		LIST COLLECTIONS
			get all available collections on the state machine
			--> do a lookup on the collection bucket and get all collections names
*/

func (sm *StateMachine) BulkApply(ops []*StateMachineOperation) ([]*StateMachineResponse, error) {
	responses := []*StateMachineResponse{}
	
	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		for _, op := range ops {
			_, createCollectionErr := sm.createCollection(root, op.Payload.Collection)
			if createCollectionErr != nil { return createCollectionErr }

			if op.Action == INSERT {
				insertResp, insertErr := sm.insertIntoCollection(root, &op.Payload)
				if insertErr != nil { return insertErr}

				insertResp.RequestID = op.RequestID

				responses = append(responses, insertResp)
			} else if op.Action == DELETE {
				deleteResp, deleteErr := sm.deleteFromCollection(root, &op.Payload)
				if deleteErr != nil { return deleteErr }

				deleteResp.RequestID = op.RequestID

				responses = append(responses, deleteResp)
			} else if op.Action == DROPCOLLECTION {
				dropResp, dropErr := sm.dropCollection(root, &op.Payload)
				if dropErr != nil { return dropErr }

				dropResp.RequestID = op.RequestID

				responses = append(responses, dropResp)
			}
		}

		return nil
	}

	bulkInsertErr := sm.DB.Update(transaction)
	if bulkInsertErr != nil { return nil, bulkInsertErr }

	return responses, nil
}

func (sm *StateMachine) Read(op *StateMachineOperation) (*StateMachineResponse, error) {
	var response *StateMachineResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		if op.Action == FIND {
			searchResp, searchErr := sm.searchInCollection(root, &op.Payload)
			if searchErr != nil { return searchErr }

			searchResp.RequestID = op.RequestID

			response = searchResp
		} else if op.Action == LISTCOLLECTIONS {
			listResp, listErr := sm.listCollections(root, &op.Payload)
			if listErr != nil { return listErr }

			listResp.RequestID = op.RequestID

			response = listResp
		}
		
		return nil
	}

	readErr := sm.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return response, nil
}

/*
	All functions below are helper functions for each of the above state machine operations
*/

func (sm *StateMachine) listCollections(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	var collections []string

	collectionBucketName := []byte(CollectionBucket)
	collectionBucket := bucket.Bucket(collectionBucketName)

	cursor := collectionBucket.Cursor()

	for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
		collections = append(collections, string(val))
	}

	return &StateMachineResponse{
		Value: strings.Join(collections, ", "),
	}, nil
}

func (sm *StateMachine) insertIntoCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	collectionName := []byte(payload.Collection)
	collection := bucket.Bucket(collectionName)

	searchIndexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }

	if searchIndexResp.Value != utils.GetZero[string]() { return searchIndexResp, nil }

	hash, hashErr := utils.GenerateRandomSHA256Hash()
	if hashErr != nil { return nil, hashErr }

	generatedKey := []byte(hash)
	value := []byte(payload.Value)

	putErr := collection.Put(generatedKey, value)
	if putErr != nil { return nil, putErr }

	insertIndexResp, insertErr := sm.insertIntoIndex(bucket, payload, generatedKey)
	if insertErr != nil { return nil, insertErr }

	return insertIndexResp, nil
}

func (sm *StateMachine) searchInCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }

	return indexResp, nil
}

func (sm *StateMachine) deleteFromCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	collectionName := []byte(payload.Collection)
	collection := bucket.Bucket(collectionName)

	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return &StateMachineResponse{ Collection: payload.Collection }, searchErr }

	delErr := collection.Delete([]byte(indexResp.Key))
	if delErr != nil { return nil, delErr }

	indexDelResp, indexDelErr := sm.deleteFromIndex(bucket, payload)
	if indexDelErr != nil { return nil, indexDelErr }

	return indexDelResp, nil
}

func (sm *StateMachine) dropCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	collectionName := []byte(payload.Collection)
	indexName := []byte(payload.Collection + IndexSuffix)

	delColErr := bucket.DeleteBucket(collectionName)
	if delColErr != nil { return nil, delColErr }

	delIndexErr := bucket.DeleteBucket(indexName)
	if delIndexErr != nil { return nil, delIndexErr }

	collectionBucketName := []byte(CollectionBucket)
	collectionBucket := bucket.Bucket(collectionBucketName)
	
	indexBucketName := []byte(IndexBucket)
	indexBucket := bucket.Bucket(indexBucketName)

	delFromColBucketErr := collectionBucket.Delete(collectionName)
	if delFromColBucketErr != nil { return nil, delFromColBucketErr }

	delFromIndexBucketErr := indexBucket.Delete(indexBucketName)
	if delFromIndexBucketErr != nil { return nil, delFromIndexBucketErr }

	return &StateMachineResponse{
		Collection: payload.Collection,
		Value: "dropped",
	}, nil
}

func (sm *StateMachine) searchInIndex(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	indexName := []byte(payload.Collection + IndexSuffix)
	index := bucket.Bucket(indexName)
	if index == nil { return &StateMachineResponse{ Collection: payload.Collection }, nil }

	indexKey := []byte(payload.Value)
	val := index.Get(indexKey)

	if val == nil { return &StateMachineResponse{ Collection: payload.Collection }, nil }

	return &StateMachineResponse{
		Collection: payload.Collection,
		Key: string(val),
		Value: payload.Value,
	}, nil
}

func (sm *StateMachine) insertIntoIndex(bucket *bolt.Bucket, payload *StateMachineOpPayload, colKey []byte) (*StateMachineResponse, error) {
	indexName := []byte(payload.Collection + IndexSuffix)
	index := bucket.Bucket(indexName)
	if index == nil { return &StateMachineResponse{ Collection: payload.Collection }, nil }

	indexKey := []byte(payload.Value)
	putErr := index.Put(indexKey, colKey)
	if putErr != nil { return nil, putErr }

	return &StateMachineResponse{
		Collection: payload.Collection,
		Key: string(colKey),
		Value: payload.Value,
	}, nil
}

func (sm *StateMachine) deleteFromIndex(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	indexName := []byte(payload.Collection + IndexSuffix)
	index := bucket.Bucket(indexName)
	if index == nil { return &StateMachineResponse{ Collection: payload.Collection }, nil }

	indexKey := []byte(payload.Value)
	val := index.Get(indexKey)
	if val == nil { return &StateMachineResponse{ Collection: payload.Collection }, nil }

	delErr := index.Delete(indexKey)
	if delErr != nil { return nil, delErr }

	return &StateMachineResponse{
		Collection: payload.Collection,
		Key: string(val),
		Value: payload.Value,
	}, nil
}

func (sm *StateMachine) createCollection(bucket *bolt.Bucket, collection string) (bool, error) {
	collectionName := []byte(collection)
	collectionBucket := bucket.Bucket(collectionName)
	
	if collectionBucket == nil {
		_, createErr := bucket.CreateBucketIfNotExists(collectionName)
		if createErr != nil { return false, createErr }
	
		indexName, createIndexErr := sm.createIndex(bucket, collection)
		if createIndexErr != nil { return false, createIndexErr }
	
		collectionBucketName := []byte(CollectionBucket)
		collectionBucket := bucket.Bucket(collectionBucketName)
	
		putCollectionErr := collectionBucket.Put(collectionName, collectionName)
		if putCollectionErr != nil { return false, putCollectionErr }
	
		indexBucketName := []byte(IndexBucket)
		indexBucket := bucket.Bucket(indexBucketName)
	
		putIndexErr := indexBucket.Put(indexName, indexName)
		if putIndexErr != nil { return false, putIndexErr }
	}

	return true, nil 
}

func (sm *StateMachine) createIndex(bucket *bolt.Bucket, collection string) ([]byte, error) {
	indexName := []byte(collection + IndexSuffix)
	_, createErr := bucket.CreateBucketIfNotExists(indexName)
	if createErr != nil { return nil, createErr }

	return indexName, nil 
}