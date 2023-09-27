package snapshot

import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/snapshotrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


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
			1.) attempt snapshot timer
				--> wait for timer to drain, signal attempt snapshot, and reset timer
			1.) snapshot signal
				--> if current leader, snapshot the state machine and then signal complete 
			2.) update snapshot for node
				--> if leader, send the latest snapshot on the system to the target follower
			3.) attempt trigger snapshot
				--> on interval, check if we can begin the snapshot process
*/

func (snpService *SnapshotService) StartSnapshotListener() {
	snpService.AttemptSnapshotTimer = time.NewTimer(AttemptSnapshotInterval)

	timeoutChan := make(chan bool)

	go func() {
		for range snpService.AttemptSnapshotTimer.C {
			timeoutChan <- true
			snpService.resetAttemptSnapshotTimer()
		}
	}()

	go func() {
		for range snpService.SnapshotStartSignal {
			if snpService.CurrentSystem.State == system.Leader { 
				snapshotErr := snpService.Snapshot() 
				if snapshotErr != nil { snpService.Log.Error("error snapshotting state and broadcasting to followers:", snapshotErr.Error()) }
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

	go func() {
		for range timeoutChan {
			if snpService.CurrentSystem.State == system.Leader && snpService.CurrentSystem.Status != system.Busy {
				_, attemptTriggerErr := snpService.AttemptTriggerSnapshot()
				if attemptTriggerErr != nil { snpService.Log.Error("error attempting to snapshot:", attemptTriggerErr.Error()) }
			}
		}
	}()
}

/*
	Attempt Trigger Snapshot
		if the size of the replicated log has exceeded the threshold determined dynamically by available space
		in the current mount and the current node is the leader, trigger a snapshot event to take a snapshot of
		the current state to store and broadcast to all followers, also pause the replicated log and let buffer 
		until the snapshot is complete. If a snapshot is performed, calculate the current system stats to update the
		dynamic threshold for snapshotting
*/

func (snpService *SnapshotService) AttemptTriggerSnapshot() (bool, error) {
	bucketSizeInBytes, getSizeErr := snpService.CurrentSystem.WAL.GetBucketSizeInBytes()
	if getSizeErr != nil { 
		snpService.Log.Error("error fetching bucket size:", getSizeErr.Error())
		return false, getSizeErr
	}

	triggerSnapshot := func() bool { 
		statsArr, getStatsErr := snpService.CurrentSystem.WAL.GetStats()
		if statsArr == nil || getStatsErr != nil { return false }

		latestObj := statsArr[len(statsArr) - 1]
		thresholdInBytes := latestObj.AvailableDiskSpaceInBytes / FractionOfAvailableSizeToTake // let's keep this small for now

		lastAppliedAtThreshold := bucketSizeInBytes >= thresholdInBytes
		systemAbleToSnapshot := snpService.CurrentSystem.State == system.Leader && snpService.CurrentSystem.Status != system.Busy
		return lastAppliedAtThreshold && systemAbleToSnapshot
	}()

	if triggerSnapshot { 
		snpService.CurrentSystem.SetStatus(system.Busy)
		select {
			case snpService.SnapshotStartSignal <- true:
			default:
		}
	}

	return true, nil
}