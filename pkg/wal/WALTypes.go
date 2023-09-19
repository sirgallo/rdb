package wal

import "sync"

import bolt "go.etcd.io/bbolt"


type WAL struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}

type SnapshotEntry struct {
	LastIncludedIndex int64
	LastIncludedTerm int64
	SnapshotFilePath string
}

type StatOP = string


const NAME = "WAL"
const SubDirectory = "raft/replog"
const FileName = "replog.db"

const Replog = "replog"
const ReplogWAL = Replog + "_wal"
const ReplogStats = Replog + "_stats"
const ReplogIndex = Replog + "_index"
const ReplogTotalElementsKey = "total"
const ReplogSizeKey = "size"

const (
	ADD StatOP = "ADD"
	SUB StatOP = "SUB"
)

const Snapshot = "snapshot"
const SnapshotKey = "currentsnapshot"

const Stats = "stats"
const MaxStats = 1000