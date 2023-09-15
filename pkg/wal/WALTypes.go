package wal

import "sync"
import bolt "go.etcd.io/bbolt"


type WAL [T comparable] struct {
	mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}


const NAME = "WAL"
const SubDirectory = "raft/replog"
const FileName = "replog.db"
const Bucket = "replog"