package statemachine

import "os"
import "path/filepath"
import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/logger"


//=========================================== State Machine


var Log = clog.NewCustomLog(NAME)


/*
	State Machine
		1.) open the db using the filepath 
		2.) create the root bucket for the state machine
		3.) create the collections for both storing all collection names and index names
			associated with the collection.
*/

func NewStateMachine() (*StateMachine, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return nil, homeErr }

	dbPath := filepath.Join(homedir, SubDirectory, DbFileName)
	
	db, openErr := bolt.Open(dbPath, 0600, nil)
	if openErr != nil { return nil, openErr }

	initTransaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		rootBucket, createRootErr := tx.CreateBucketIfNotExists(rootName)
		if createRootErr != nil { return createRootErr }

		collectiontName := []byte(CollectionBucket)
		_, createColErr := rootBucket.CreateBucketIfNotExists(collectiontName)
		if createColErr != nil { return createColErr }

		indexName := []byte(IndexBucket)
		_, createIndexErr := rootBucket.CreateBucketIfNotExists(indexName)
		if createIndexErr != nil { return createIndexErr }
		return nil
	}

	bucketErrInit := db.Update(initTransaction)
	if bucketErrInit != nil { return nil, bucketErrInit }

  return &StateMachine{ 
		DBFile: dbPath,
		DB: db,
	}, nil
}