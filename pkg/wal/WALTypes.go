package wal

import "os"
import "sync"


type WAL struct {
	mutex sync.Mutex
	File *os.File
	WriteStream chan []byte
}