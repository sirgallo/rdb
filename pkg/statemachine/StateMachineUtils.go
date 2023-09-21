package statemachine

import "io"
import "os"
import "path/filepath"

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Snapshot Utils


/*
	Snapshot State Machine
		1.) generate the name for the snapshot file, which is db name and a uuid 
		2.) open a new file for the snapshot to be written to
		3.) open up a gzip stream to compress the file
		4.) create a bolt transaction to write to the gzip stream
		5.) if successful, return the snapshot path
*/

func (sm *StateMachine) SnapshotStateMachine() (string, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return utils.GetZero[string](), homeErr }

	snapshotFileName, fileNameErr := sm.generateFilename()
	if fileNameErr != nil { return utils.GetZero[string](), fileNameErr }

	snapshotPath := filepath.Join(homedir, SubDirectory, snapshotFileName)

	snapshotFile, fCreateErr := os.Create(snapshotPath)
	if fCreateErr != nil { return utils.GetZero[string](), fCreateErr }
	defer snapshotFile.Close()

	transaction := func(tx *bolt.Tx) error {
		_, writeErr := tx.WriteTo(snapshotFile)
		if writeErr != nil { return writeErr }

		return nil
	}

	snapshotErr := sm.DB.View(transaction)
	if snapshotErr != nil { return utils.GetZero[string](), snapshotErr }

	return snapshotPath, nil
}

/*
	Replay Snapshot
		1.) using the filepath for the latest snapshot, open the file
		2.) create a gzip reader
		3.) close the db and remove the original db file
		4.) recreate the same file and read the snapshot
		5.) write the content of the snapshot to the newly created db file
		6.) reopen the db
*/

func (sm *StateMachine) ReplaySnapshot(snapshotPath string) error {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return homeErr }

	dbPath := filepath.Join(homedir, SubDirectory, DbFileName)

	snapshotFile, openErr := os.Open(snapshotPath)
	if openErr != nil { return openErr }
	defer snapshotFile.Close()

	closeErr := sm.DB.Close()
	if closeErr != nil { return closeErr }

	removeFilerr := os.Remove(dbPath)
	if removeFilerr != nil && ! os.IsNotExist(removeFilerr) { return removeFilerr }
	
	databaseFile, createFileErr := os.Create(dbPath)
	if createFileErr != nil { return createFileErr }
	defer databaseFile.Close()

	_, copyErr := io.Copy(databaseFile, snapshotFile)
	if copyErr != nil { return copyErr }

	db, openErr := bolt.Open(dbPath, 0600, nil)
	if openErr != nil { return openErr }

	sm.DB = db

	return nil
}

/*
	generate Filename
		--> generate the snapshot name, which is dbname_uniqueID
*/

func (sm *StateMachine) generateFilename() (string, error) {
	hash, hashErr := utils.GenerateRandomSHA256Hash()
	if hashErr != nil { return utils.GetZero[string](), hashErr }

	snapshotName := func() string { return FileNamePrefix + "_" + hash }()

	return snapshotName, nil
}