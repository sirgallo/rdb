package wal

import "os"
import "path/filepath"

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"


//=========================================== Write Ahead Log


var Log = clog.NewCustomLog(NAME)

/*
	Write Ahead Log
		1.) open the db using the filepath 
		2.) create the replog bucket if it does not already exist
*/

func NewWAL [T log.MachineCommands]() (*WAL[T], error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return nil, homeErr }

	dbPath := filepath.Join(homedir, SubDirectory, FileName)
	
	db, openErr := bolt.Open(dbPath, 0600, nil)
	if openErr != nil { return nil, openErr }

	replogTransaction := func(tx *bolt.Tx) error {
		bucketName := []byte(Bucket)
		_, createErr := tx.CreateBucketIfNotExists(bucketName)
		if createErr != nil { return createErr }

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

  return &WAL[T]{ 
		DBFile: dbPath,
		DB: db,
	}, nil
}