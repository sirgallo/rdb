package replog

import "context"
import "sync"
import "sync/atomic"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Leader


/*
	Heartbeat:
		for all systems in the System Map, send an empty AppendEntryRPC

		If a higher term is discovered in a response, revert the Leader back to Follower State
*/

func (rlService *ReplicatedLogService[T]) Heartbeat() {
	aliveSystems, _ := rlService.GetAliveSystemsAndMinSuccessResps()
	rlRespChans := rlService.createRLRespChannels()
	requests := []ReplicatedLogRequest{}
	successfulResps := int64(0)
	
	for _, sys := range aliveSystems {
		request := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: rlService.prepareAppendEntryRPC(sys.NextIndex, true),
		}

		requests = append(requests, request)
	}

	var hbWG sync.WaitGroup

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()

		for {
			select {
				case <- *rlRespChans.BroadcastClose:
					return
				case <- *rlRespChans.SuccessChan:
					atomic.AddInt64(&successfulResps, 1)
				case term :=<- *rlRespChans.HigherTermDiscovered:
					rlService.Log.Warn("higher term discovered.")
					rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
					rlService.LeaderAcknowledgedSignal <- true
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

	close(*rlRespChans.SuccessChan)
	close(*rlRespChans.HigherTermDiscovered)
}

/*
	Replicate Logs:
		1.) append the new log to the replicated log on the Leader,
			--> index is just the index of the last log + 1
			--> term is the current term on the leader
		2.) prepare AppendEntryRPCs for each system in the Systems Map
			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
		3.) on responses
			--> if the Leader receives a success signal from the majority of the nodes in the cluster, 
				commit the logs to the state machine
			--> if the Leader gets a response with a higher term than its own, revert to Follower state
*/

func (rlService *ReplicatedLogService[T]) ReplicateLogs(cmd T) {
	rlRespChans := rlService.createRLRespChannels()
	aliveSystems, minSuccessfulResps := rlService.GetAliveSystemsAndMinSuccessResps()
	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	requests := []ReplicatedLogRequest{}
	
	newLog := &system.LogEntry[T]{
		Index: lastLogIndex + 1,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: cmd,
	}

	rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)

	for _, sys := range aliveSystems {
		request := ReplicatedLogRequest{
			Host: sys.Host,
			AppendEntry: rlService.prepareAppendEntryRPC(sys.NextIndex, false),
		}
		
		requests = append(requests, request)
	}

	successfulResps := int64(0)
	
	var repLogWG sync.WaitGroup

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()

		for {
			select {
				case <- *rlRespChans.BroadcastClose:
					if successfulResps >= int64(minSuccessfulResps) { 
						rlService.Log.Warn("at least minimum successful responses received:", successfulResps)
						rlService.CommitLogsLeader()
					} else { rlService.Log.Warn("minimum successful responses not received.") }
					return
				case <- *rlRespChans.SuccessChan:
					atomic.AddInt64(&successfulResps, 1)
				case term :=<- *rlRespChans.HigherTermDiscovered:
					rlService.Log.Warn("higher term discovered.")
					rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
					rlService.LeaderAcknowledgedSignal <- true
					return
			}
		}
	}()

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()
		rlService.broadcastAppendEntryRPC(requests, rlRespChans)
	}()

	repLogWG.Wait()

	close(*rlRespChans.SuccessChan)
	close(*rlRespChans.HigherTermDiscovered)
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
*/

func (rlService *ReplicatedLogService[T]) broadcastAppendEntryRPC(requestsPerHost []ReplicatedLogRequest, rlRespChans RLResponseChannels) {
	defer close(*rlRespChans.BroadcastClose)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var appendEntryWG sync.WaitGroup

	for _, req := range requestsPerHost {
		appendEntryWG.Add(1)

		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()
			sys, _ := rlService.Systems.Load(req.Host)

			conn, connErr := rlService.ConnectionPool.GetConnection(sys.(*system.System[T]).Host, rlService.Port)
			if connErr != nil { 
				rlService.Log.Error("Failed to connect to", sys.(*system.System[T]).Host + rlService.Port, ":", connErr) 
				return
			}

			select {
				case <- ctx.Done():
					rlService.ConnectionPool.PutConnection(sys.(*system.System[T]).Host, conn)
					return
				default:	
					res, err := rlService.clientAppendEntryRPC(conn, sys.(*system.System[T]), req)
					if err != nil { return }

					if res.Success {
						*rlRespChans.SuccessChan <- 1
					} else {
						if res.Term > rlService.CurrentSystem.CurrentTerm { 
							rlService.Log.Warn("higher term found on response for AppendEntryRPC:", res.Term)
							rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{
								CurrentTerm: &res.Term,
							})
						
							*rlRespChans.HigherTermDiscovered <- res.Term	
							cancel()
						}
					}

					rlService.ConnectionPool.PutConnection(sys.(*system.System[T]).Host, conn)
			}
		}(req)
	}

	appendEntryWG.Wait()
}

func (rlService *ReplicatedLogService[T]) clientAppendEntryRPC(
	conn *grpc.ClientConn, 
	sys *system.System[T], req ReplicatedLogRequest,
) (*replogrpc.AppendEntryResponse, error) {
	client := replogrpc.NewRepLogServiceClient(conn)

	appendEntryRPC := func () (*replogrpc.AppendEntryResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Millisecond)
		defer cancel()
		
		res, err := client.AppendEntryRPC(ctx, req.AppendEntry)
		if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
		return res, nil
	}
	
	maxRetries := 5
	expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
	expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)
			
	res, err := expBackoff.PerformBackoff(appendEntryRPC)
	if err != nil { 
		rlService.Log.Warn("system", sys.Host, "unreachable, removing from registered systems.")
		rlService.Systems.Delete(sys.Host)
		rlService.ConnectionPool.CloseConnections(sys.Host)
		
		return nil, err
	}

	sys.NextIndex = res.LatestLogIndex
	rlService.Systems.Store(sys.Host, sys)

	return res, nil
}

func (rlService *ReplicatedLogService[T]) createRLRespChannels() RLResponseChannels {
	broadcastClose := make(chan struct{})
	successChan := make(chan int)
	higherTermDiscovered := make(chan int64)

	return RLResponseChannels{
		BroadcastClose: &broadcastClose,
		SuccessChan: &successChan,
		HigherTermDiscovered: &higherTermDiscovered,
	}
}