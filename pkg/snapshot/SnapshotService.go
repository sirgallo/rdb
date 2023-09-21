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
			3.) process incoming snapshot
				--> when a snapshot is received from a leader, process it in a separate go routine
*/

func (snpService *SnapshotService) StartSnapshotListener() {
	go func() {
		for range snpService.SnapshotStartSignal {
			if snpService.CurrentSystem.State == system.Leader { 
				snapshotErr := snpService.Snapshot() 
				if snapshotErr != nil { 
					snpService.Log.Error("error snapshotting state and broadcasting to followers:", snapshotErr.Error()) 
					snpService.SnapshotCompleteSignal <- true
				}
			}
		}
	}()

	go func() {
		for host := range snpService.UpdateSnapshotForSystemSignal {
			if snpService.CurrentSystem.State == system.Leader {
				go func(host string) { 
					updateErr := snpService.UpdateIndividualSystem(host)
					if updateErr != nil { snpService.Log.Error("error updating individual system:", updateErr) }
				}(host)
			}
		}
	}()
}