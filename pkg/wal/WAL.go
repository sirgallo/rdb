package wal

import "os"
import "path/filepath"

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/logger"


//=========================================== Write Ahead Log


var Log = clog.NewCustomLog(NAME)

/*
	Write Ahead Log
		1.) open the db using the filepath 
		2.) create the replog bucket if it does not already exist
		4.) also create both a stats bucket and an index bucket sub bucket
			--> the replog stats bucket contains both the total size of the replicated log and total entries
			--> the index bucket conains the first known entry for each term, which is used to 
					reduce the number of failed AppendEntryRPCs when a node is brought into the cluster
					and being synced back to the leader
		5.) create the snapshot bucket
			--> this contains a reference to the filepath for the most up to date snapshot for the cluster
		6.) create the stats bucket
			--> the stats bucket contains a time series of system stats as the system progresses
*/

func NewWAL () (*WAL, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return nil, homeErr }

	dbPath := filepath.Join(homedir, SubDirectory, FileName)
	
	db, openErr := bolt.Open(dbPath, 0600, nil)
	if openErr != nil { return nil, openErr }

	replogTransaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Replog)
		parent, createErr := tx.CreateBucketIfNotExists(bucketName)
		if createErr != nil { return createErr }

		walBucketName := []byte(ReplogWAL)
		_, walCreateErr := parent.CreateBucketIfNotExists(walBucketName)
		if walCreateErr != nil { return createErr }

		statsBucketName := []byte(ReplogStats)
		_, statsCreateErr := parent.CreateBucketIfNotExists(statsBucketName)
		if statsCreateErr != nil { return statsCreateErr }

		indexBucketName := []byte(ReplogIndex)
		_, indexCreateErr := parent.CreateBucketIfNotExists(indexBucketName)
		if indexCreateErr != nil { return indexCreateErr }

		return nil
	}

	bucketErrRepLog := db.Update(replogTransaction)
	if bucketErrRepLog != nil { return nil, bucketErrRepLog }

	snapshotTransaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Snapshot)
		_, createErr := tx.CreateBucketIfNotExists(bucketName)
		if createErr != nil { return createErr }

		return nil
	}

	bucketErrSnapshot := db.Update(snapshotTransaction)
	if bucketErrSnapshot != nil { return nil, bucketErrSnapshot }

	statsTransaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Stats)
		_, createErr := tx.CreateBucketIfNotExists(bucketName)
		if createErr != nil { return createErr }

		return nil
	}

	bucketErrStats := db.Update(statsTransaction)
	if bucketErrStats != nil { return nil, bucketErrStats }

  return &WAL{ 
		DBFile: dbPath,
		DB: db,
	}, nil
}