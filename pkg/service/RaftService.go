package service

import "log"
import "net"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Raft Service


// NOTE: Incomplete

/*
	initialize both the leader election module and the replicated log module under the same raft service
	and link together

	initialize the state machine operations/state machine dependency
*/

func NewRaftService [T any, U string, V comparable](opts RaftServiceOpts[T, U, V]) *RaftService[T, U, V] {	
	leConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rlConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)

	systemFilter := func(sys *system.System[statemachine.StateMachineOperation[U, V]]) bool { 
		return sys.Host != opts.CurrentSystem.Host 
	}

	leOpts := &leaderelection.LeaderElectionOpts[statemachine.StateMachineOperation[U, V]]{
		Port:           opts.Ports.LeaderElection,
		ConnectionPool: leConnPool,
		CurrentSystem:  opts.CurrentSystem,
		SystemsList:    utils.Filter[*system.System[statemachine.StateMachineOperation[U, V]]](opts.SystemsList, systemFilter),
	}

	rlOpts := &replog.ReplicatedLogOpts[statemachine.StateMachineOperation[U, V]]{
		Port:           opts.Ports.ReplicatedLog,
		ConnectionPool: rlConnPool,
		CurrentSystem:  opts.CurrentSystem,
		SystemsList:    utils.Filter[*system.System[statemachine.StateMachineOperation[U, V]]](opts.SystemsList, systemFilter),
	}

	leService := leaderelection.NewLeaderElectionService[statemachine.StateMachineOperation[U, V]](leOpts)
	rlService := replog.NewReplicatedLogService[statemachine.StateMachineOperation[U, V]](rlOpts)

	smOpMap := opts.StateMachineClient.NewStateMachineOperationMap(opts.StateMachineClientConstructor)

	return &RaftService[T, U, V]{
		Protocol: opts.Protocol,
		Ports: opts.Ports,
		CurrentSystem: opts.CurrentSystem,
		SystemsList: opts.SystemsList,

		LeaderElection: leService,
		ReplicatedLog: rlService,
		StateMachineOperations: *smOpMap,
	}
}

/*
	Start Raft Service:
		start both the leader election and replicated log modules.
		initialize the state machine dependency and link to the replicated log module for log commits
*/

func (raft *RaftService[T, U, V]) StartRaftService() {
	leListener, err := net.Listen("tcp", utils.NormalizePort(raft.Ports.LeaderElection))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

	rlListener, err := net.Listen("tcp", utils.NormalizePort(raft.Ports.ReplicatedLog))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

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
			cmd :=<- raft.ClientCommandChannel
			raft.ReplicatedLog.AppendLogSignal <- cmd
		}
	}()

	go func () {
		for {
			logs :=<- raft.ReplicatedLog.LogCommitChannel
			
			for idx, log := range logs {
				_, opErr := raft.StateMachineOperations.Ops[log.LogEntry.Command.Action](log.LogEntry.Command)
				if opErr != nil { break }
				
				log.Complete = true
				logs[idx] = log
			}
			
			raft.ReplicatedLog.LogCommitChannel <- logs
		}
	}()

	select{}
}