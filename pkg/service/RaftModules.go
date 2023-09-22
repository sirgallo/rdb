package service

import "github.com/sirgallo/raft/pkg/system"


//=========================================== Raft Modules


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

	/*
	go func() {
		for cmd := range raft.Relay.RelayedAppendLogSignal {
			raft.ReplicatedLog.AppendLogSignal <- cmd
		}
	}()
	*/

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

	/*
	go func() {
		for response := range raft.ForwardResp.ForwardRespChannel {
			if raft.CurrentSystem.State == system.Follower {
				if response.RequestOrigin == raft.CurrentSystem.Host { 
					raft.HTTPService.ResponseChannel <- response 
				}
			}
		}
	}()
	*/

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