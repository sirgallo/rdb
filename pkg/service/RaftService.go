package service

import "net"
import "os"
import "sync"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/relay"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/snapshot"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/wal"


//=========================================== Raft Service


const NAME = "Raft"
var Log = clog.NewCustomLog(NAME)


/*
	initialize sub modules under the same raft service and link together
*/

func NewRaftService[T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State](opts RaftServiceOpts[T, U, V, W]) *RaftService[T, U, V, W] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	wal, walErr := wal.NewWAL[T]()
	if walErr != nil { Log.Fatal("unable to create or open WAL") }

	currentSystem := &system.System[T]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: DefaultCommitIndex,
		LastApplied: DefaultLastApplied,
		Status: system.Ready,
		WAL: wal,
	}

	raft := &RaftService[T, U, V, W]{
		Protocol: opts.Protocol,
		Systems: &sync.Map{},
		CurrentSystem: currentSystem,
		CommandChannel: make(chan T, CommandChannelBuffSize),
		StateMachineLogApplyChan: make(chan replog.LogCommitChannelEntry[T]),
		StateMachineLogAppliedChan: make(chan error),
	}

	for _, sys := range opts.SystemsList {
		sys.UpdateNextIndex(0)
		sys.SetStatus(system.Ready)
		
		raft.Systems.Store(sys.Host, sys)
	}

	leConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rlConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	snpConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)

	leOpts := &leaderelection.LeaderElectionOpts[T]{
		Port: opts.Ports.LeaderElection,
		ConnectionPool: leConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rlOpts := &replog.ReplicatedLogOpts[T]{
		Port: opts.Ports.ReplicatedLog,
		ConnectionPool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rOpts := &relay.RelayOpts[T]{
		Port: opts.Ports.Relay,
		ConnectionPool: rConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	snpOpts := &snapshot.SnapshotServiceOpts[T, U, V, W]{
		Port: opts.Ports.Snapshot,
		ConnectionPool: snpConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
		StateMachine: opts.StateMachine,
		SnapshotHandler: opts.SnapshotHandler,
		// SnapshotReplayer: opts.SnapshotReplayer,
	}

	leService := leaderelection.NewLeaderElectionService[T](leOpts)
	rlService := replog.NewReplicatedLogService[T](rlOpts)
	rService := relay.NewRelayService[T](rOpts)
	snpService := snapshot.NewSnapshotService[T, U, V, W](snpOpts)

	raft.LeaderElection = leService
	raft.ReplicatedLog = rlService
	raft.Relay = rService
	raft.Snapshot = snpService

	return raft
}

/*
	Start Raft Service:
		start all sub modules and create go routines to link appropriate channels

		1.) start log apply go routine and update state machine on startup
			go routine:
				on new logs to apply to the state machine, pass through to the state machine
				and once processed, return successful and failed logs to the log commit channel

				--> needs to be started before log updates can be applied

			update replicated logs on startup

		2.) start http net listeners and all sub modules
		3.) start module pass throughs 
*/

func (raft *RaftService[T, U, V, W]) StartRaftService() {
	go func() {
		for {
			logs :=<- raft.ReplicatedLog.LogApplyChan
			completedLogs := []replog.LogCommitChannelEntry[T]{}

			for _, log := range logs {
				raft.StateMachineLogApplyChan <- log
				applyErr := <- raft.StateMachineLogAppliedChan

				if applyErr != nil {
					Log.Debug("error on op:", applyErr.Error())
					log.Complete = false
				} else { log.Complete = true }

				completedLogs = append(completedLogs, log)
			}

			raft.ReplicatedLog.LogApplyChan <- completedLogs
		}
	}()

	var updateStateMachineMutex sync.Mutex
	updateStateMachineMutex.Lock()

	_, updateErr := raft.UpdateRepLogOnStartup()
	if updateErr != nil { Log.Error("error on log replication:", updateErr.Error()) }

	updateStateMachineMutex.Unlock()

	raft.StartModules()
	raft.StartModulePassThroughs()
	
	select {}
}

/*
	Update RepLog On Startup:
		on system startup or restart replay the WAL
			1.) get the latest log from the WAL on disk
			2.) update commit index to last log index from synced WAL --> WAL only contains committed logs
			3.) update current term to term of last log
*/

func (raft *RaftService[T, U, V, W]) UpdateRepLogOnStartup() (bool, error) {
	snapshot, snapshotErr := raft.CurrentSystem.WAL.GetSnapshot()
	if snapshotErr != nil { return false, snapshotErr }
	if snapshot != nil { 
		_, replayErr := raft.SnapshotReplayer(raft.StateMachine, snapshot) 
		if replayErr != nil { return false, replayErr }
	}

	lastLog, latestErr := raft.CurrentSystem.WAL.GetLatest()

	if latestErr != nil {
		return false, latestErr
	} else if lastLog != nil {
		raft.CurrentSystem.CommitIndex = lastLog.Index
		raft.CurrentSystem.CurrentTerm = lastLog.Term

		applyErr := raft.ReplicatedLog.ApplyLogs()
		if applyErr != nil { return false, applyErr }

		total, totalErr := raft.CurrentSystem.WAL.GetTotal(int64(0), lastLog.Index)
		if totalErr == nil { Log.Error("error on get total from WAL", totalErr) }

		Log.Info("total entries on startup:", total)
	}

	return true, nil
}

/*
	Start Modules
		initialize net listeners and start all sub modules
*/

func (raft *RaftService[T, U, V, W]) StartModules() {
	leListener, leErr := net.Listen(raft.Protocol, raft.LeaderElection.Port)
	if leErr != nil { Log.Error("Failed to listen: %v", leErr.Error()) }

	rlListener, rlErr := net.Listen(raft.Protocol, raft.ReplicatedLog.Port)
	if rlErr != nil { Log.Error("Failed to listen: %v", rlErr.Error()) }

	rListener, rErr := net.Listen(raft.Protocol, raft.Relay.Port)
	if rErr != nil { Log.Error("Failed to listen: %v", rErr.Error()) }

	snpListener, snpErr := net.Listen(raft.Protocol, raft.Snapshot.Port)
	if snpErr != nil { Log.Error("Failed to listen: %v", snpErr.Error()) }

	go raft.ReplicatedLog.StartReplicatedLogService(&rlListener)
	go raft.LeaderElection.StartLeaderElectionService(&leListener)
	go raft.Relay.StartRelayService(&rListener)
	go raft.Snapshot.StartSnapshotService(&snpListener)
}

/*
	Start Module Pass Throughs
		go routine 1:
			on acknowledged signal from rep log module, attempt reset timeout on
			leader election module
		go routine 2:
			on signal from successful leader election, force heartbeat on log module
		go routine 3:
			on relay from follower, pass new command to leader to be appended to log
		go routine 4:
			on command channel for new commands from client, determine whether or not
			to pass to rep log module to be appended if leader, otherwise relay from
			follower to current leader	
*/

func (raft *RaftService[T, U, V, W]) StartModulePassThroughs() {
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

	go func() {
		for {
			<- raft.ReplicatedLog.SignalSnapshot
			raft.Snapshot.SnapshotSignal <- true
		}
	}()
}