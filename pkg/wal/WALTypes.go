package wal

import "sync"

import bolt "go.etcd.io/bbolt"


type WAL [T comparable] struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}


const NAME = "WAL"
const SubDirectory = "raft/replog"
const FileName = "replog.db"

const Replog = "replog"
const ReplogWAL = Replog + "_wal"
const ReplogStats = Replog + "_stats"
const ReplogTotalElementsKey = "total"
const ReplogSizeKey = "size"

const Snapshot = "snapshot"
const SnapshotKey = "currentsnapshot"

const Stats = "stats"
const MaxStats = 1000