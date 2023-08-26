package replog

import "context"
import "log"
import "sync"
import "sync/atomic"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Leader


/*
	Heartbeat:
		for all systems that have status Alive, send an empty AppendEntryRPC
*/

func (rlService *ReplicatedLogService[T]) Heartbeat() {
	rlRespChans := rlService.createRLRespChannels()
	sysHostPtr := &rlService.CurrentSystem.Host
	requests := []ReplicatedLogRequest{}
	aliveSystems, _ := rlService.GetAliveSystemsAndMinSuccessResps()
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	
	if lastLogIndex == -1 {
		lastLogIndex = utils.GetZero[int64]()
		lastLogTerm = utils.GetZero[int64]()
	}

	for _, sys := range aliveSystems {
		var request ReplicatedLogRequest
		
		request.Host = sys.Host
		request.AppendEntry = &replogrpc.AppendEntry{
			Term: rlService.CurrentSystem.CurrentTerm,
			LeaderId: *sysHostPtr,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm: lastLogTerm,
			Entries: nil,
			LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
		}

		requests = append(requests, request)
	}

	batchedRequests := rlService.truncatedRequests(requests)

	var hbWG sync.WaitGroup

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()

		for {
			select {
				case <- *rlRespChans.BroadcastClose:
					return
				case <- *rlRespChans.SuccessChan:
				case <- *rlRespChans.HigherTermDiscovered:
					log.Println("higher term discovered.")
					rlService.LeaderAcknowledgedSignal <- true
					return
			}
		}
	}()

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		rlService.broadcastAppendEntryRPC(batchedRequests, rlRespChans)
	}()

	hbWG.Wait()
}

/*
	Replicate Logs:
		1.) append the new log to the replicated log on the leader,
			--> index is just the index of the last log + 1
			--> term is the current term on the leader
		2.) determine what systems are alive and prepare AppendEntryRPCs for each
			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
		3.) on responses
			--> if the leader receives a success signal from the majority of the nodes in the cluster, 
				commit the logs to the state machine
*/

func (rlService *ReplicatedLogService[T]) ReplicateLogs(cmd T) {	
	rlRespChans := rlService.createRLRespChannels()
	aliveSystems, minSuccessfulResps := rlService.GetAliveSystemsAndMinSuccessResps()
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	requests := []ReplicatedLogRequest{}
	
	newLog := &system.LogEntry[T]{
		Index: lastLogIndex + 1,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: cmd,
	}

	rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)

	for _, sys := range aliveSystems {
		var request ReplicatedLogRequest
		request.Host = sys.Host

		appendEntry := rlService.prepareAppendEntryRPC(lastLogIndex, lastLogTerm)

		request.AppendEntry = appendEntry
		requests = append(requests, request)
	}

	batchedRequests := rlService.truncatedRequests(requests)
	successfulResps := int64(0)
	
	var repLogWG sync.WaitGroup

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()

		for {
			select {
				case <- *rlRespChans.BroadcastClose:
					if successfulResps >= int64(minSuccessfulResps) { 
						log.Println("success resps meets minimum required for log commit")
						rlService.CommitLogsLeader()
					} else { log.Println("min successful responses not received.") }
					return
				case <- *rlRespChans.SuccessChan:
					atomic.AddInt64(&successfulResps, 1)
				case term :=<- *rlRespChans.HigherTermDiscovered:
					log.Println("higher term discovered.")
					rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{
						CurrentTerm: &term,
					})

					rlService.LeaderAcknowledgedSignal <- true
					return
			}
		}
	}()

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()
		rlService.broadcastAppendEntryRPC(batchedRequests, rlRespChans)
	}()

	repLogWG.Wait()
}

/*
	Shared Broadcast RPC function:
		utilized by all three functions above
	
		for requests to be broadcasted:
			1.) send AppendEntryRPCs in parallel to each follower in the cluster
			2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
			3.) 
				if err: set the system status to dead and return
				if res:
					if success: 
						--> update total successful replies and update the next index of the system to the last log index of the reply
					else if failure and the reply has higher term than the leader: 
						--> update the state of the leader to follower, recognize a higher term and thus a more correct log
						--> signal that a higher term has been discovered and cancel all leftover requests
*/

func (rlService *ReplicatedLogService[T]) broadcastAppendEntryRPC(requestsPerHost []ReplicatedLogRequest, rlRespChans RLResponseChannels) {	
	defer close(*rlRespChans.BroadcastClose)
	
	signalStopRPC := make(chan bool)
	stopRPC := make(chan struct{})

	defer close(signalStopRPC)
	
	go func() {
		<- signalStopRPC
		close(stopRPC)
	}()

	var appendEntryWG sync.WaitGroup

	for _, reqs := range requestsPerHost {
		appendEntryWG.Add(1)

		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			receivingHost := req.Host
			conn, connErr := rlService.ConnectionPool.GetConnection(receivingHost, rlService.Port)
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", receivingHost + rlService.Port, connErr) }

			client := replogrpc.NewRepLogServiceClient(conn)

			appendEntryRPC := func () (*replogrpc.AppendEntryResponse, error) {
				res, err := client.AppendEntryRPC(context.Background(), req.AppendEntry)
				if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
				return res, nil
			}

			func() {
				select {
					case <- stopRPC:
						return
					default:			
						maxRetries := 5
						expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
						expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)
			
						res, err := expBackoff.PerformBackoff(appendEntryRPC)
			
						sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { 
							return sys.Host == receivingHost
						})[0]
			
						if err != nil { 
							log.Printf("setting sytem %s to status dead", receivingHost)
							system.SetStatus[T](sys, false)
							return
						}
			
						sys.NextIndex = res.LatestLogIndex
			
						if res.Success {
							*rlRespChans.SuccessChan <- 1
						} else {
							if res.Term > rlService.CurrentSystem.CurrentTerm { 
								log.Println("higher term found on response for AppendEntryRPC", res.Term)
								rlService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{
									CurrentTerm: &res.Term,
								})
							
								*rlRespChans.HigherTermDiscovered <- res.Term
								signalStopRPC <- true
							}
						}
				}
			}()
			

			rlService.ConnectionPool.PutConnection(receivingHost, conn)
		}(reqs)
	}

	appendEntryWG.Wait()
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