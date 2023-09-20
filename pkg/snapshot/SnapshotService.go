package snapshot

import "net"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Snapshot Service


/*
	create a new service instance with passable options
*/

func NewSnapshotService(opts *SnapshotServiceOpts) *SnapshotService {
	snpService := &SnapshotService{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		SnapshotStartSignal: make(chan bool),
		SnapshotCompleteSignal: make(chan bool),
		UpdateSnapshotForSystemSignal: make(chan string),
		Log: *clog.NewCustomLog(NAME),
	}

	return snpService
}

/*
	start the snapshot service:
		--> launch the grpc server for SnapshotRPC
		--> start the start the snapshot listener
*/

func (snpService *SnapshotService) StartSnapshotService(listener *net.Listener) {
	srv := grpc.NewServer()
	snpService.Log.Info("snapshot gRPC server is listening on port:", snpService.Port)
	snapshotrpc.RegisterSnapshotServiceServer(srv, snpService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { snpService.Log.Error("Failed to serve:", err.Error()) }
	}()

	snpService.StartSnapshotListener()
}

/*
	Snapshot Listener:
		separate go routines:
			1.) snapshot signal
				--> if current leader, snapshot the state machine and then signal complete 
			2.) update snapshot for node
				--> if leader, send the latest snapshot on the system to the target follower
*/

func (snpService *SnapshotService) StartSnapshotListener() {
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