package service

import "sync"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/request"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/snapshot"
import "github.com/sirgallo/raft/pkg/system"


type RaftPortOpts struct {
	RequestService int
	LeaderElection int
	ReplicatedLog int
	Snapshot int
}

type RaftServiceOpts struct {
	Protocol string
	Ports RaftPortOpts
	SystemsList []*system.System
	ConnPoolOpts connpool.ConnectionPoolOpts
}

type RaftService struct {
	Protocol string
	Ports RaftPortOpts
	
	CurrentSystem *system.System
	Systems *sync.Map

	LeaderElection *leaderelection.LeaderElectionService
	ReplicatedLog *replog.ReplicatedLogService
	Snapshot *snapshot.SnapshotService
	RequestService *request.RequestService
}


const DefaultCommitIndex = -1
const DefaultLastApplied = -1
const CommandChannelBuffSize = 100000