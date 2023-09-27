package service

import "net"

import "github.com/sirgallo/raft/pkg/system"


//=========================================== Raft Modules


/*
	Start Modules
		initialize net listeners and start all sub modules
		modules:
			1. replicated log module
			2. leader election module
			3. snapshot module
			4. http request module
*/

func (raft *RaftService) StartModules() {
	leListener, leErr := net.Listen(raft.Protocol, raft.LeaderElection.Port)
	if leErr != nil { Log.Error("Failed to listen: %v", leErr.Error()) }

	rlListener, rlErr := net.Listen(raft.Protocol, raft.ReplicatedLog.Port)
	if rlErr != nil { Log.Error("Failed to listen: %v", rlErr.Error()) }

	snpListener, snpErr := net.Listen(raft.Protocol, raft.Snapshot.Port)
	if snpErr != nil { Log.Error("Failed to listen: %v", snpErr.Error()) }

	go raft.ReplicatedLog.StartReplicatedLogService(&rlListener)
	go raft.LeaderElection.StartLeaderElectionService(&leListener)
	go raft.Snapshot.StartSnapshotService(&snpListener)
	go raft.RequestService.StartRequestService()
}

/*
	Start Module Pass Throughs
		go routine 1:
			on acknowledged signal from rep log module, attempt reset timeout on
			leader election module
		go routine 2:
			on signal from successful leader election, force heartbeat on log module
		go routine 3:
			on responses from state machine ops, pass to request channel to be sent to
			the client
		go routine 4:
			on signal from replicated log module to start snapshot process, send signal 
			to the snapshot module
		go routine 5:
			on signal from the replicated log module that a follower needs the most up
			to date snapshot, signal the snapshot module to send to that follower
*/

func (raft *RaftService) StartModulePassThroughs() {
	go func() {
		for range raft.ReplicatedLog.LeaderAcknowledgedSignal {
			raft.LeaderElection.ResetTimeoutSignal <- true
		}
	}()

	go func() {
		for range raft.LeaderElection.HeartbeatOnElection {
			raft.ReplicatedLog.ForceHeartbeatSignal <- true
		}
	}()

	go func() {
		for cmdEntry := range raft.RequestService.RequestChannel {
			if raft.CurrentSystem.State == system.Leader {
				raft.ReplicatedLog.AppendLogSignal <- cmdEntry
			}
		}
	}()

	go func() {
		for response := range raft.ReplicatedLog.StateMachineResponseChannel {
			if raft.CurrentSystem.State == system.Leader {
				raft.RequestService.ResponseChannel <- response
			}
		}
	}()

	go func() {
		for range raft.ReplicatedLog.SignalStartSnapshot {
			raft.Snapshot.SnapshotStartSignal <- true
		}
	}()

	go func() {
		for host := range raft.ReplicatedLog.SendSnapshotToSystemSignal {
			raft.ReplicatedLog.SendSnapshotToSystemSignal <- host
		}
	}()
}