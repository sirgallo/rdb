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
			cmdEntry :=<- raft.HTTPService.RequestChannel
			if raft.CurrentSystem.State == system.Leader {
				raft.ReplicatedLog.AppendLogSignal <- cmdEntry
			} else { raft.Relay.RelayChannel <- cmdEntry }
		}
	}()

	go func() {
		for {
			response :=<- raft.ReplicatedLog.StateMachineResponseChannel
			if raft.CurrentSystem.State == system.Leader {
				if response.RequestOrigin == raft.CurrentSystem.Host {
					raft.HTTPService.ResponseChannel <- response
				} else { raft.ForwardResp.LeaderRelayResponseChannel <- response }
			}
		}
	}()

	go func() {
		for {
			response :=<- raft.ForwardResp.ForwardRespChannel
			if raft.CurrentSystem.State == system.Follower {
				if response.RequestOrigin == raft.CurrentSystem.Host { 
					raft.HTTPService.ResponseChannel <- response 
				}
			}
		}
	}()

	go func() {
		for {
			<- raft.ReplicatedLog.SignalStartSnapshot
			raft.Snapshot.SnapshotStartSignal <- true
		}
	}()

	go func() {
		for {
			<- raft.Snapshot.SnapshotCompleteSignal
			raft.ReplicatedLog.SignalCompleteSnapshot <- true
		}
	}()

	go func() {
		for {
			host :=<- raft.ReplicatedLog.SendSnapshotToSystemSignal
			raft.ReplicatedLog.SendSnapshotToSystemSignal <- host
		}
	}()
}