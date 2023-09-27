package service

import "os"
import "sync"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/request"
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

func NewRaftService(opts RaftServiceOpts) *RaftService {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	wal, walErr := wal.NewWAL()
	if walErr != nil { Log.Fatal("unable to create or open WAL") }

	sm, smErr := statemachine.NewStateMachine()
	if smErr != nil { Log.Fatal("unable to create or open State Machine") }

	currentSystem := &system.System{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: DefaultCommitIndex,
		LastApplied: DefaultLastApplied,
		Status: system.Ready,
		WAL: wal,
		StateMachine: sm,
	}

	raft := &RaftService{
		Protocol: opts.Protocol,
		Systems: &sync.Map{},
		CurrentSystem: currentSystem,
	}

	for _, sys := range opts.SystemsList {
		sys.UpdateNextIndex(0)
		sys.SetStatus(system.Ready)
		
		raft.Systems.Store(sys.Host, sys)
	}

	leConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	rlConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)
	snpConnPool := connpool.NewConnectionPool(opts.ConnPoolOpts)

	reqOpts := &request.RequestServiceOpts{
		Port: opts.Ports.RequestService,
		CurrentSystem: currentSystem,
	}

	leOpts := &leaderelection.LeaderElectionOpts{
		Port: opts.Ports.LeaderElection,
		ConnectionPool: leConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	rlOpts := &replog.ReplicatedLogOpts{
		Port: opts.Ports.ReplicatedLog,
		ConnectionPool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	snpOpts := &snapshot.SnapshotServiceOpts{
		Port: opts.Ports.Snapshot,
		ConnectionPool: snpConnPool,
		CurrentSystem: currentSystem,
		Systems: raft.Systems,
	}

	httpService := request.NewRequestService(reqOpts)
	leService := leaderelection.NewLeaderElectionService(leOpts)
	rlService := replog.NewReplicatedLogService(rlOpts)
	snpService := snapshot.NewSnapshotService(snpOpts)

	raft.RequestService = httpService
	raft.LeaderElection = leService
	raft.ReplicatedLog = rlService
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

		2.) start all sub modules
		3.) start module pass throughs 
*/

func (raft *RaftService) StartRaftService() {
	var updateStateMachineMutex sync.Mutex
	updateStateMachineMutex.Lock()

	_, updateErr := raft.UpdateRepLogOnStartup()
	if updateErr != nil { Log.Error("error on log replication:", updateErr.Error()) }

	statsErr := raft.InitStats()
	if statsErr != nil { Log.Error("error fetching initial stats", statsErr.Error()) }

	updateStateMachineMutex.Unlock()

	raft.StartModules()
	raft.StartModulePassThroughs()
	
	select {}
}