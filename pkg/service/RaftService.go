package service

import "log"
import "net"
import "os"
import "sync"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/relay"
import "github.com/sirgallo/raft/pkg/system"


//=========================================== Raft Service


/*
	initialize both the leader election module and the replicated log module under the same raft service
	and link together

	initialize the state machine operations/state machine dependency
*/


const NAME = "Raft"
var Log = clog.NewCustomLog(NAME)

func NewRaftService [T system.MachineCommands](opts RaftServiceOpts[T]) *RaftService[T] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	currentSystem := &system.System[T]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		LastApplied: 0,
		Replog: []*system.LogEntry[T]{},
	}

	raft := &RaftService[T]{
		Protocol: opts.Protocol,
		Systems: &sync.Map{},
		CurrentSystem: currentSystem,
		CommandChannel: make(chan T, 100000),
	}

	for _, sys := range opts.SystemsList {
		raft.Systems.Store(sys.Host, sys)
	}

	leConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rlConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)

	leOpts := &leaderelection.LeaderElectionOpts[T]{
		Port: opts.Ports.LeaderElection,
		ConnectionPool: leConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rlOpts := &replog.ReplicatedLogOpts[T]{
		Port:	opts.Ports.ReplicatedLog,
		ConnectionPool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rOpts := &relay.RelayOpts[T]{
		Port:	opts.Ports.Relay,
		ConnectionPool: rConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	leService := leaderelection.NewLeaderElectionService[T](leOpts)
	rlService := replog.NewReplicatedLogService[T](rlOpts)
	rService := relay.NewRelayService[T](rOpts)

	raft.LeaderElection = leService
	raft.ReplicatedLog = rlService
	raft.Relay = rService

	return raft
}

/*
	Start Raft Service:
		start both the leader election and replicated log modules.
		initialize the state machine dependency and link to the replicated log module for log commits
*/

func (raft *RaftService[T]) StartRaftService() {
	leListener, leErr := net.Listen(raft.Protocol, raft.LeaderElection.Port)
	if leErr != nil { Log.Error("Failed to listen: %v", leErr) }

	rlListener, rlErr := net.Listen(raft.Protocol, raft.ReplicatedLog.Port)
	if rlErr != nil { Log.Error("Failed to listen: %v", rlErr) }

	rListener, rErr := net.Listen(raft.Protocol, raft.Relay.Port)
	if rErr != nil { Log.Error("Failed to listen: %v", rErr) }

	go raft.ReplicatedLog.StartReplicatedLogService(&rlListener)
	go raft.LeaderElection.StartLeaderElectionService(&leListener)
	go raft.Relay.StartRelayService(&rListener)

	go func() {
		for {
			<- raft.ReplicatedLog.LeaderAcknowledgedSignal
			raft.LeaderElection.ResetTimeoutSignal <- true
		}
	}()

	go func() {
		for {
			<- raft.LeaderElection.HeartbeatOnElection
			raft.ReplicatedLog.ForceHeartbeatSignal <- true
		}
	}()

	go func() {
		for {
			cmd :=<- raft.Relay.RelayedAppendLogSignal
			raft.ReplicatedLog.AppendLogSignal <- cmd
		}
	}()

	go func() {
		for {
			cmdEntry :=<- raft.CommandChannel
			if raft.CurrentSystem.State == system.Leader {
				raft.ReplicatedLog.AppendLogSignal <- cmdEntry
			} else { raft.Relay.RelayChannel <- cmdEntry }
		}
	}()
	
	select{}
}