package statemachine

import bolt "go.etcd.io/bbolt"
import "github.com/google/uuid"


func (sm *StateMachine) Find(payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	var resp *StateMachineResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		searchResp, searchErr := sm.searchInCollection(root, payload)
		if searchErr != nil { return searchErr }

		resp = searchResp

		return nil
	}

	findErr := sm.DB.View(transaction)
	if findErr != nil { return nil, findErr }

	return resp, nil
}

func (sm *StateMachine) Insert(payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	var resp *StateMachineResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		_, createCollectionErr := sm.createCollection(root, payload.Collection)
		if createCollectionErr != nil { return createCollectionErr }

		insertResp, insertErr := sm.insertIntoCollection(root, payload)
		if insertErr != nil { return insertErr}

		resp = insertResp

		return nil
	}

	insertErr := sm.DB.Update(transaction)
	if insertErr != nil { return nil, insertErr }

	return resp, nil
}

func (sm *StateMachine) BulkApply(ops []*StateMachineOperation) (bool, error) {
	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		for _, op := range ops {
			_, createCollectionErr := sm.createCollection(root, op.Payload.Collection)
			if createCollectionErr != nil { return createCollectionErr }

			if op.Action == INSERT {
				_, insertErr := sm.insertIntoCollection(root, &op.Payload)
				if insertErr != nil { return insertErr}
			} else if op.Action == DELETE {
				_, deleteErr := sm.deleteFromCollection(root, &op.Payload)
				if deleteErr != nil { return deleteErr }
			} else if op.Action == DROPCOLLECTION {
				dropErr := sm.dropCollection(root, &op.Payload)
				if dropErr != nil { return dropErr }
			}
		}

		return nil
	}

	bulkInsertErr := sm.DB.Update(transaction)
	if bulkInsertErr != nil { return false, bulkInsertErr }

	return true, nil
}

func (sm *StateMachine) Delete(payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	var resp *StateMachineResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		_, createCollectionErr := sm.createCollection(root, payload.Collection)
		if createCollectionErr != nil { return createCollectionErr }

		deleteResp, deleteErr := sm.deleteFromCollection(root, payload)
		if deleteErr != nil { return deleteErr }

		resp = deleteResp

		return nil
	}

	deleteErr := sm.DB.Update(transaction)
	if deleteErr != nil { return nil, deleteErr }

	return resp, nil
}

func (sm *StateMachine) DropCollection(payload *StateMachineOpPayload) (bool, error) {
	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)
		
		dropErr := sm.dropCollection(root, payload)
		if dropErr != nil { return dropErr }

		return nil
	}

	dropErr := sm.DB.Update(transaction)
	if dropErr != nil { return false, dropErr }

	return true, nil
}

func (sm *StateMachine) ListCollections(payload *StateMachineOpPayload) ([]string, error) {
	var resp []string

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		collectionBucketName := []byte(CollectionBucket)
		collectionBucket := root.Bucket(collectionBucketName)

		cursor := collectionBucket.Cursor()

		for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
			resp = append(resp, string(val))
		}

		return nil
	}

	listErr := sm.DB.View(transaction)
	if listErr != nil { return nil, listErr }

	return resp, nil
}

func (sm *StateMachine) generateKey() []byte {
	uuid := uuid.New()
	return []byte(uuid.String())
}

func (sm *StateMachine) insertIntoCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	collectionName := []byte(payload.Collection)
	collection := bucket.Bucket(collectionName)

	searchIndexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }

	if searchIndexResp != nil { return searchIndexResp, nil }

	generatedKey := sm.generateKey()
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
	if searchErr != nil { return nil, searchErr }

	delErr := collection.Delete([]byte(indexResp.Key))
	if delErr != nil { return nil, delErr }

	indexDelResp, indexDelErr := sm.deleteFromIndex(bucket, payload)
	if indexDelErr != nil { return nil, indexDelErr }

	return indexDelResp, nil
}

func (sm *StateMachine) dropCollection(bucket *bolt.Bucket, payload *StateMachineOpPayload) error {
	collectionName := []byte(payload.Collection)
	indexName := []byte(payload.Collection + IndexSuffix)

	delColErr := bucket.DeleteBucket(collectionName)
	if delColErr != nil { return delColErr }

	delIndexErr := bucket.DeleteBucket(indexName)
	if delIndexErr != nil { return delIndexErr }

	collectionBucketName := []byte(CollectionBucket)
	collectionBucket := bucket.Bucket(collectionBucketName)
	
	indexBucketName := []byte(IndexBucket)
	indexBucket := bucket.Bucket(indexBucketName)

	delFromColBucketErr := collectionBucket.Delete(collectionName)
	if delFromColBucketErr != nil { return delFromColBucketErr }

	delFromIndexBucketErr := indexBucket.Delete(indexBucketName)
	if delFromIndexBucketErr != nil { return delFromIndexBucketErr }

	return nil
}

func (sm *StateMachine) searchInIndex(bucket *bolt.Bucket, payload *StateMachineOpPayload) (*StateMachineResponse, error) {
	indexName := []byte(payload.Collection + IndexSuffix)
	index := bucket.Bucket(indexName)
	if index == nil { return nil, nil }

	indexKey := []byte(payload.Value)
	val := index.Get(indexKey)

	if val == nil { return nil, nil }

	return &StateMachineResponse{
		Collection: payload.Collection,
		Key: string(val),
		Value: payload.Value,
	}, nil
}

func (sm *StateMachine) insertIntoIndex(bucket *bolt.Bucket, payload *StateMachineOpPayload, colKey []byte) (*StateMachineResponse, error) {
	indexName := []byte(payload.Collection + IndexSuffix)
	index := bucket.Bucket(indexName)
	if index == nil { return nil, nil }

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
	if index == nil { return nil, nil }

	indexKey := []byte(payload.Value)
	val := index.Get(indexKey)
	if val == nil { return nil, nil }

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