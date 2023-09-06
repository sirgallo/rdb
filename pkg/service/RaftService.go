package service

import "log"
import "net"
import "os"
import "sync"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"


//=========================================== Raft Service


/*
	initialize both the leader election module and the replicated log module under the same raft service
	and link together

	initialize the state machine operations/state machine dependency
*/


const NAME = "Raft"
var Log = clog.NewCustomLog(NAME)

func NewRaftService [T comparable](opts RaftServiceOpts[T]) *RaftService[T] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	currentSystem := &system.System[T]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		LastApplied: 0,
		Replog: []*system.LogEntry[T]{},
	}
	
	cpOpts := connpool.ConnectionPoolOpts{ MaxConn: 10 }

	raft := &RaftService[T]{
		Protocol: opts.Protocol,
		Systems: &sync.Map{},
		CurrentSystem: currentSystem,
	}

	for _, sys := range opts.SystemsList {
		raft.Systems.Store(sys.Host, sys)
	}

	rlConnPool := connpool.NewConnectionPool(cpOpts)
	leConnPool := connpool.NewConnectionPool(cpOpts)

	lePort := 54321
	rlPort := 54322

	rlOpts := &replog.ReplicatedLogOpts[T]{
		Port:	rlPort,
		ConnectionPool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	leOpts := &leaderelection.LeaderElectionOpts[T]{
		Port: lePort,
		ConnectionPool: leConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rlService := replog.NewReplicatedLogService[T](rlOpts)
	leService := leaderelection.NewLeaderElectionService[T](leOpts)

	raft.LeaderElection = leService
	raft.ReplicatedLog = rlService

	return raft
}

/*
	Start Raft Service:
		start both the leader election and replicated log modules.
		initialize the state machine dependency and link to the replicated log module for log commits
*/

func (raft *RaftService[T]) StartRaftService() {
	leListener, err := net.Listen("tcp", raft.LeaderElection.Port)
	if err != nil { Log.Error("Failed to listen: %v", err) }

	rlListener, err := net.Listen("tcp", raft.ReplicatedLog.Port)
	if err != nil { Log.Error("Failed to listen: %v", err) }

	go raft.ReplicatedLog.StartReplicatedLogService(&rlListener)
	go raft.LeaderElection.StartLeaderElectionService(&leListener)

	go func () {
		for {
			<- raft.ReplicatedLog.LeaderAcknowledgedSignal
			raft.LeaderElection.ResetTimeoutSignal <- true
		}
	}()

	go func () {
		for {
			<- raft.LeaderElection.HeartbeatOnElection
			raft.ReplicatedLog.ForceHeartbeatSignal <- true
		}
	}()
	
	select{}
}