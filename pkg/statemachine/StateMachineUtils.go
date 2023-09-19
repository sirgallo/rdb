package statemachine

import "compress/gzip"
import "io"
import "os"
import "path/filepath"
import "github.com/google/uuid"

import bolt "go.etcd.io/bbolt"

import "github.com/sirgallo/raft/pkg/utils"


func (sm *StateMachine) SnapshotStateMachine() (string, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return utils.GetZero[string](), homeErr }

	snapshotFileName := sm.generateFilename()
	snapshotPath := filepath.Join(homedir, SubDirectory, snapshotFileName)

	snapshotFile, fCreateErr := os.Create(snapshotPath)
	if fCreateErr != nil { return utils.GetZero[string](), fCreateErr }
	
	defer snapshotFile.Close()

	gzipWriter := gzip.NewWriter(snapshotFile)
	
	defer gzipWriter.Close()

	transaction := func(tx *bolt.Tx) error {
		_, writeErr := tx.WriteTo(gzipWriter)
		if writeErr != nil { return writeErr }

		return nil
	}

	snapshotErr := sm.DB.Update(transaction)
	if snapshotErr != nil { return utils.GetZero[string](), snapshotErr }

	return snapshotPath, nil
}

func (sm *StateMachine) ReplaySnapshot(snapshotPath string) error {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return homeErr }

	dbPath := filepath.Join(homedir, SubDirectory, FileName)

	gzippedSnapshotFile, openErr := os.Open(snapshotPath)
	if openErr != nil { return openErr }
	
	defer gzippedSnapshotFile.Close()

	gzipReader, gzipErr := gzip.NewReader(gzippedSnapshotFile)
	if gzipErr != nil { return gzipErr }
	
	defer gzipReader.Close()

	closeErr := sm.DB.Close()
	if closeErr != nil { return closeErr }

	removeFilerr := os.Remove(dbPath)
	if removeFilerr != nil && ! os.IsNotExist(removeFilerr) { return removeFilerr }
	
	databaseFile, createFileErr := os.Create(dbPath)
	if createFileErr != nil { return createFileErr }
	
	defer databaseFile.Close()

	_, copyErr := io.Copy(databaseFile, gzipReader)
	if copyErr != nil { return copyErr }

	db, openErr := bolt.Open(dbPath, 0600, nil)
	if openErr != nil { return openErr }

	sm.DB = db

	return nil
}


func (sm *StateMachine) generateFilename() string {
	id := uuid.New()

	return FileName + "_" + id.String()
}