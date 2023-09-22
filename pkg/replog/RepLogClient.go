package replog

import "context"
import "sync"
import "sync/atomic"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Client


/*
	Heartbeat:
		for all systems in the System Map, send an empty AppendEntryRPC

		If a higher term is discovered in a response, revert the Leader back to Follower State
*/

func (rlService *ReplicatedLogService) Heartbeat() error {
	rlRespChans := rlService.createRLRespChannels()
	aliveSystems, _ := rlService.GetAliveSystemsAndMinSuccessResps()

	defer close(rlRespChans.SuccessChan)
	defer close(rlRespChans.HigherTermDiscovered)

	requests := []ReplicatedLogRequest{}
	successfulResps := int64(0)

	for _, sys := range aliveSystems {
		preparedEntries, prepareErr := rlService.PrepareAppendEntryRPC(sys.NextIndex, true)
		if prepareErr != nil { return prepareErr }

		request := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: preparedEntries,
		}

		requests = append(requests, request)
	}

	var hbWG sync.WaitGroup

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()

		for {
			select {
				case <- rlRespChans.BroadcastClose:
					return
				case <- rlRespChans.SuccessChan:
					atomic.AddInt64(&successfulResps, 1)
				case term :=<- rlRespChans.HigherTermDiscovered:
					rlService.Log.Warn("higher term discovered.")
					rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
					rlService.attemptLeadAckSignal()
					return
			}
		}
	}()

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		
		rlService.broadcastAppendEntryRPC(requests, rlRespChans)
	}()

	hbWG.Wait()

	return nil
}

/*
	Replicate Logs:
		1.) append the new log to the WAL on the Leader,
			--> index is just the index of the last log + 1
			--> term is the current term on the leader
		2.) prepare AppendEntryRPCs for each system in the Systems Map
			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
		3.) on responses
			--> if the Leader receives a success signal from the majority of the nodes in the cluster, apply the logs to the state machine
			--> if a response with a higher term than its own, revert to Follower state
			--> if a response with a last log index less than current log index on leader, sync logs until up to date
*/

func (rlService *ReplicatedLogService) ReplicateLogs(cmd *statemachine.StateMachineOperation) error {
	rlRespChans := rlService.createRLRespChannels()
	aliveSystems, minSuccessfulResps := rlService.GetAliveSystemsAndMinSuccessResps()

	defer close(rlRespChans.SuccessChan)
	defer close(rlRespChans.HigherTermDiscovered)

	lastLogIndex, _, lastLogErr := rlService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { return lastLogErr }

	nextIndex := lastLogIndex + 1

	requests := []ReplicatedLogRequest{}
	successfulResps := int64(0)

	newLog := &log.LogEntry{
		Index: nextIndex,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: *cmd,
	}

	appendErr := rlService.CurrentSystem.WAL.Append(newLog)
	if appendErr != nil {
		rlService.Log.Error("append error:", appendErr.Error())
		return appendErr 
	}

	for _, sys := range aliveSystems {
		preparedEntries, prepareErr := rlService.PrepareAppendEntryRPC(sys.NextIndex, false)
		if prepareErr != nil { 
			rlService.Log.Error("prepare entries rpc error:", prepareErr.Error())
			return prepareErr 
		}

		request := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: preparedEntries,
		}

		requests = append(requests, request)
	}

	var repLogWG sync.WaitGroup

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()

		for {
			select {
				case <- rlRespChans.BroadcastClose:
					if successfulResps >= int64(minSuccessfulResps) {
						rlService.Log.Info("at least minimum successful responses received:", successfulResps)
						rlService.Log.Info("applying logs to state machine and appending to write ahead log")

						rlService.CurrentSystem.IncrementCommitIndex()
						applyErr := rlService.ApplyLogs()
						if applyErr != nil { rlService.Log.Error("error applying command to state machine:", applyErr.Error()) }
					} else { rlService.Log.Warn("minimum successful responses not received.") }
					
					return
				case <- rlRespChans.SuccessChan:
					atomic.AddInt64(&successfulResps, 1)
				case term :=<- rlRespChans.HigherTermDiscovered:
					rlService.Log.Warn("higher term discovered.")
					rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
					rlService.attemptLeadAckSignal()
					return
			}
		}
	}()

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()
		
		broadcastErr := rlService.broadcastAppendEntryRPC(requests, rlRespChans)
		if broadcastErr != nil { rlService.Log.Error("error on broadcast AppendEntryRPC", broadcastErr.Error()) }
	}()

	repLogWG.Wait()

	return nil
}

/*
	Shared Broadcast RPC function:
		utilized by all three functions above

		for requests to be broadcasted:
			1.) send AppendEntryRPCs in parallel to each follower in the cluster
			2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
			3.)
				if err: remove system from system map and close all connections -- it has failed
				if res:
					if success:
						--> update total successful replies and update the next index of the system to the last log index of the reply
					else if failure and the reply has higher term than the leader:
						--> update the state of the leader to follower, recognize a higher term and thus a more correct log
						--> signal that a higher term has been discovered and cancel all leftover requests
					otherwise if failure:
						--> sync the follower up to the leader for any inconsistent log entries
*/

func (rlService *ReplicatedLogService) broadcastAppendEntryRPC(requestsPerHost []ReplicatedLogRequest, rlRespChans RLResponseChannels) error {
	defer close(rlRespChans.BroadcastClose)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	var appendEntryWG sync.WaitGroup

	for _, req := range requestsPerHost {
		appendEntryWG.Add(1)

		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			s, _ := rlService.Systems.Load(req.Host)
			sys := s.(*system.System)

			conn, connErr := rlService.ConnectionPool.GetConnection(sys.Host, rlService.Port)
			if connErr != nil {
				rlService.Log.Error("Failed to connect to", sys.Host + rlService.Port, ":", connErr.Error())
				return
			}

			select {
				case <- ctx.Done():
					rlService.ConnectionPool.PutConnection(sys.Host, conn)
					return
				default:
					res, err := rlService.clientAppendEntryRPC(conn, sys, req)
					if err != nil { return }

					if res.Success {
						rlRespChans.SuccessChan <- 1
					} else {
						if res.Term > rlService.CurrentSystem.CurrentTerm {
							rlService.Log.Warn("higher term found on response for AppendEntryRPC:", res.Term)
							rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &res.Term })

							rlRespChans.HigherTermDiscovered <- res.Term
							cancel()
						} else {
							rlService.Log.Warn("preparing to sync logs for:", sys.Host)
							rlService.SyncLogChannel <- sys.Host
						}
					}

					rlService.ConnectionPool.PutConnection(sys.Host, conn)
			}
		}(req)
	}

	appendEntryWG.Wait()

	return nil
}

/*
	Client Append Entry RPC:
		helper method for making individual rpc calls

		perform exponential backoff
		--> success: update system NextIndex and return result
		--> error: remove system from system map and close all open connections
*/

func (rlService *ReplicatedLogService) clientAppendEntryRPC(
	conn *grpc.ClientConn,
	sys *system.System,
	req ReplicatedLogRequest,
) (*replogrpc.AppendEntryResponse, error) {
	client := replogrpc.NewRepLogServiceClient(conn)

	appendEntryRPC := func() (*replogrpc.AppendEntryResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
		defer cancel()

		res, err := client.AppendEntryRPC(ctx, req.AppendEntry)
		if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
		return res, nil
	}

	maxRetries := 3
	expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
	expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)

	res, err := expBackoff.PerformBackoff(appendEntryRPC)
	if err != nil {
		rlService.Log.Warn("system", sys.Host, "unreachable, setting status to dead")

		sys.SetStatus(system.Dead)
		rlService.ConnectionPool.CloseConnections(sys.Host)

		return nil, err
	}

	sys.UpdateNextIndex(res.NextLogIndex)

	return res, nil
}

func (rlService *ReplicatedLogService) createRLRespChannels() RLResponseChannels {
	broadcastClose := make(chan struct{})
	successChan := make(chan int)
	higherTermDiscovered := make(chan int64)

	return RLResponseChannels{
		BroadcastClose: broadcastClose,
		SuccessChan: successChan,
		HigherTermDiscovered: higherTermDiscovered,
	}
}

func (rlService *ReplicatedLogService) attemptLeadAckSignal() {
	select {
		case rlService.LeaderAcknowledgedSignal <- true:
		default:
	}
}