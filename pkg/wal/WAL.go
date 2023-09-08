package wal

import "bytes"
import "path/filepath"
import "os"

import "github.com/sirgallo/raft/pkg/logger"


//=========================================== Write Ahead Log


const SubDirectory = "raft/replog"
const FileName = "replog.wal"

const NAME = "WAL"
var Log = clog.NewCustomLog(NAME)

func NewWAL() (*WAL, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return nil, homeErr }

	filepath := filepath.Join(homedir, SubDirectory, FileName)
	
	file, openErr := os.OpenFile(filepath, os.O_APPEND| os.O_CREATE| os.O_RDWR, 0644)
	if openErr != nil { return nil, openErr }

  return &WAL{ 
		File: file,
		WriteStream: make(chan []byte, 10000),
	}, nil
}

/*
	Start Write Stream:
		in a separate go function, pipe new entries through the write stream channel
		--> this should improve performance
*/

func (wal *WAL) StartWriteStream() {
	go func() {
		for {
			data :=<- wal.WriteStream
			writeErr := wal.Append(data)
			if writeErr != nil { Log.Error("write err:", writeErr.Error()) }
		}
	}()
}

/*
	Append:
		append new entries to the WAL
			1.) create a header that contains the length of the data that is 4 bytes
			2.) append the header to the front of the data
			3.) write the new entry to the last line of the WAL
*/

func (wal *WAL) Append(data []byte) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	_, writeErr := wal.File.Write(data)
	if writeErr != nil { return writeErr }
	
	return nil
}

/*
	Replay:
		on system restarts, replay log from durable storage
			1.) get file content
			2.) if len 0, just return
			3.) otherwise, split on new lines and return the lines
*/

func (wal *WAL) Replay() ([][]byte, error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	content, readErr := os.ReadFile(wal.File.Name())
	
	if len(content) == 0 { return nil, nil }
	if readErr != nil { return nil, readErr }

	lines := bytes.Split(content, []byte{'\n'})

	return lines[:len(lines) - 1], nil	// remove null line at end
}

/*
	Close:
		closes the file 
*/

func (wal *WAL) Close() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	return wal.File.Close()
}