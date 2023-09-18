package snapshot

import "net"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Leader Election Service


/*
	create a new service instance with passable options
	--> initialize state to Follower and initialize a random timeout period for leader election
*/

func NewSnapshotService[T log.MachineCommands, U statemachine.Action, V statemachine.Data, W statemachine.State](opts *SnapshotServiceOpts[T, U, V, W]) *SnapshotService[T, U, V, W] {
	snpService := &SnapshotService[T, U, V, W]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		SnapshotStartSignal: make(chan bool),
		SnapshotCompleteSignal: make(chan bool),
		UpdateSnapshotForSystemSignal: make(chan string),
		StateMachine: opts.StateMachine,
		SnapshotHandler: opts.SnapshotHandler,
		Log: *clog.NewCustomLog(NAME),
	}

	return snpService
}

/*
	start the snapshot service:
		--> launch the grpc server for SnapshotRPC
		--> start the start the snapshot listener
*/

func (snpService *SnapshotService[T, U, V, W]) StartSnapshotService(listener *net.Listener) {
	srv := grpc.NewServer()
	snpService.Log.Info("snapshot gRPC server is listening on port:", snpService.Port)
	snapshotrpc.RegisterSnapshotServiceServer(srv, snpService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { snpService.Log.Error("Failed to serve:", err.Error()) }
	}()

	snpService.StartSnapshotListener()
}

func (snpService *SnapshotService[T, U, V, W]) StartSnapshotListener() {
	go func() {
		for {
			<- snpService.SnapshotStartSignal
			if snpService.CurrentSystem.State == system.Leader { 
				snpService.Snapshot() 
				snpService.SnapshotCompleteSignal <- true
			}
		}
	}()

	go func() {
		for {
			host :=<- snpService.UpdateSnapshotForSystemSignal
			if snpService.CurrentSystem.State == system.Leader {
				go snpService.UpdateIndividualSystem(host)
			}
		}
	}()
}